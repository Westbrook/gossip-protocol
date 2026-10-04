"""Prospective final-M4 execution of the 12 originally authored M2 histories.

Each action has its own durable request, raw response and paused SQLite capture.
Original M2 expected answers stay on the host; the child receives inputs only.
The source-specific review, admission, external checkpoint and fresh execution
are independent requirements. A snapshot follows the response and is not proof
of atomic method return, process-death durability, or a forced transaction race.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import platform
import queue
import subprocess
import tempfile
import threading
from typing import Any
import uuid

from . import candidate_storage_product_execution_v1 as transport
from . import candidate_storage_driver_v1 as b01
from . import candidate_intake_store_driver_v3 as b02
from . import candidate_m2_product_profile_v1 as profile
from . import candidate_m2_review_authority_v1 as review
from . import candidate_observation_admission_v1 as admission
from . import candidate_execution_journal_v1 as journals
from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_emergency_cleanup_v1 as cleanup
from . import candidate_client_process_v4 as process
from . import candidate_source_capture_policy_v1 as source_capture
from .gitstore import GitStore
from .sandbox import DockerValidator
from . import candidate_storage_prestart_v1 as prestart
from . import project_acceptance_registry_v1 as registry

PROTOCOL = 'candidate-m2-product-execution-v1-ascii-json-v1-prestart-v2-desktop-inputs-v1'
MAPPED_PROTOCOL = PROTOCOL + '-' + profile.finite.M2_MAPPING
TARGET_CONTRACT = transport.TARGET_CONTRACT
FAMILY = 'm2-direct-api'
SOURCE_CAPTURE_POLICY = source_capture.BatchCapturePolicy()
LIMITS = chain.Limits()
CHUNK_BYTES = transport.CHUNK_BYTES
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
encoded, digest = profile.encoded, profile.digest
ExecutionError, ExecutionUnknown = transport.ExecutionError, transport.ExecutionUnknown
require, sha = transport.require, transport.sha
_verify_regular_tree = transport._verify_regular_tree


def evaluator_sources() -> dict[str, str]:
    result = transport.evaluator_sources()
    result.update(profile.finite.sources())
    result.update(source_capture.evaluator_sources())
    names: tuple[str, ...] = ('candidate_m2_product_execution_v1.py', 'candidate_m2_product_profile_v1.py',
             'candidate_m2_product_observation_v1.py', 'candidate_m2_review_authority_v1.py',
             'library_m2_acceptance_cases_v1.py', 'library_v2_inherited_cases_v1.py', 'sandbox.py')
    names += tuple(profile.inherited.PRIOR_SOURCE_SHA256)
    root = Path(__file__).resolve().parent
    result.update({'gossip_harness/' + name: sha((root / name).read_bytes()) for name in names})
    admission.verify_loaded_sources(result)
    return dict(sorted(result.items()))


def capture_git_source(store: GitStore, commit_oid: str) -> tuple[str, dict[str, bytes]]:
    """The new M2 owner always selects exact reviewed batch60/cleanup5 capture.

    No callback, legacy selection or cached capture crosses an effect boundary.
    SourceCaptureUnavailable remains an infrastructure error for the reader.
    """
    return source_capture.capture_registered_source(store, commit_oid, policy=SOURCE_CAPTURE_POLICY)


@dataclass(frozen=True, slots=True)
class M2Policy:
    timeout_seconds: int = 30
    image_id: str = b01.RUNTIME_IMAGE
    seed: str = 'fixed-public-m2-histories-v1'

    def __post_init__(self) -> None:
        require(type(self.timeout_seconds) is int and 1 <= self.timeout_seconds <= 120, 'Invalid timeout')
        require(self.image_id == b01.RUNTIME_IMAGE, 'Exact retained image required')
        registry.identifier(self.seed)


@dataclass(frozen=True, slots=True)
class M2Binding:
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
        require(self.protocol in (PROTOCOL, MAPPED_PROTOCOL) and self.milestone == 'M4'
            and self.requirements_sha256 == TARGET_CONTRACT, 'Explicit final-M4 contract required')
        require(self.family == FAMILY and self.purpose in registry.PURPOSES, 'Closed M2 family/purpose required')
        for name, value in asdict(self).items():
            if name.endswith('_sha256'):
                registry.sha256(value)
        registry.identifier(self.case_id)


def mapping_profile_for_protocol(protocol: str) -> str | None:
    require(type(protocol) is str and protocol in (PROTOCOL, MAPPED_PROTOCOL), 'Closed M2 execution protocol required')
    return None if protocol == PROTOCOL else profile.finite.M2_MAPPING


def profile_for_binding(binding: M2Binding) -> profile.M2Profile:
    require(type(binding) is M2Binding, 'Exact M2 binding required')
    value = profile.profile_for(binding.case_id, binding.purpose,
        mapping_profile=mapping_profile_for_protocol(binding.protocol))
    require(value.sha256 == binding.profile_sha256 and digest(value.record()) == binding.definition_sha256,
            'M2 mapping marker/profile/definition differs')
    return value


def fixture_identity(case_id: str) -> str:
    return digest({'adapter_files': admission.source_manifest(profile.adapter_files(case_id)),
                   'input_fixtures': profile.input_fixtures(case_id),
                   'recipe': profile.recipe_for(case_id)})


def binding_for(files: dict[str, bytes], value: profile.M2Profile, policy: M2Policy, runtime: dict[str, Any],
                plan: review.LayoutPlan, *, review_authority: review.M2ReviewAuthority) -> M2Binding:
    require(profile.accepted_profile(value) and type(policy) is M2Policy
        and review.accepted_layout_plan(plan) and type(review_authority) is review.M2ReviewAuthority,
        'Exact M2 profile, policy and original layout authority required')
    provenance = review_authority.provenance(plan)
    actual = profile.reconstruct(value)
    require(value == actual, 'Exact source-derived M2 profile required')
    native = profile.source_sha256(files)
    require(plan.source_sha256 == admission.source_sha256(files) and plan.native_source_sha256 == native
        and plan.family == FAMILY and plan.case_id == value.case_id and plan.profile_sha256 == value.sha256
        and plan.purpose == value.purpose and plan.mapping_profile == value.mapping_profile
        and plan.schedule == 'ordinary_public_operations',
        'Source, native identity, profile, purpose or independently reviewed layout differs')
    return M2Binding(admission.source_sha256(files), TARGET_CONTRACT, 'M4', value.purpose,
        FAMILY, value.case_id, native, digest(value.record()), value.sha256, fixture_identity(value.case_id),
        review_authority.enrollment.report_sha256, digest(provenance), digest(evaluator_sources()), digest(runtime),
        digest({'environment': DockerValidator._environment(),
            'host_python': [platform.python_implementation(), platform.python_version()],
            'snapshot_protocol': b01.SNAPSHOT_PROTOCOL, 'volume_options': b01.VOLUME_OPTIONS}),
        digest({'policy': asdict(policy), 'source_capture_policy': SOURCE_CAPTURE_POLICY.record(),
            'journal': asdict(LIMITS), 'chunk_bytes': CHUNK_BYTES, 'prestart_policy': prestart.definition(),
            'capture_bytes': b02.MAX_CAPTURE_BYTES, 'original_stream_bytes': b02.MAX_STREAM_BYTES,
            'action_count': len(value.phases), 'cleanup': asdict(cleanup.CleanupLimits())}),
        digest({'seed': policy.seed}), protocol=PROTOCOL if value.mapping_profile is None else MAPPED_PROTOCOL)


def mechanics_case_id(value: profile.M2Profile) -> str:
    return FAMILY + ':' + value.case_id + ':mechanics'


def gate_for(subject: registry.Subject, binding: M2Binding, *, gate_id: str) -> registry.Gate:
    require(type(binding) is M2Binding, 'Exact M2 binding required')
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
class M2Registration:
    binding: M2Binding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    gate: registry.Gate
    cohort_trajectory_ids: tuple[str, ...]


def observation_registration(registration: M2Registration) -> admission.ObservationRegistration:
    require(type(registration) is M2Registration, 'Exact M2 registration required')
    binding = registration.binding
    require(registration.gate == gate_for(registration.gate.binding.subject, binding,
            gate_id=registration.gate.gate_id), 'Registered M2 gate differs')
    return admission.ObservationRegistration(registration.gate, registration.commit_oid,
        registration.tree_oid, registration.repetition_id, registration.cohort_trajectory_ids,
        binding.definition_sha256, binding.profile_sha256, profile.ORIGINAL_DEFINITION_PURPOSE,
        admission.binding_sha256(binding, gate=registration.gate))


class _Commands(transport._Commands):
    def run(self, label: str, arguments: list[str], limit: int = b01.MAX_STREAM_BYTES) -> dict[str, Any]:
        require(self.owner.mode == 'physical', 'Fixture owners never dispatch Docker commands')
        return super().run(label, arguments, limit)


class _WireSession(b02._Session):
    """Reuse bounded stream drains; retain original action lines before parsing."""
    commands: _Commands

    def phase(self, phase: str) -> Any:
        owner = self.commands.owner
        require(phase in owner.profile.phases, 'Only the declared ordered M2 actions are allowed')
        index = owner.profile.phases.index(phase)
        require(index == len(self.requests), 'M2 action replay/reordering refused')
        require(not self.errors and all(count <= b02.MAX_STREAM_BYTES for count in self.counts.values()),
                'Session output bound')
        assert self.process.stdin is not None
        request = (phase + '\n').encode()
        self.process.stdin.write(request)
        self.process.stdin.flush()
        self.requests.append({'ordinal': index, 'phase': phase, 'request_sha256': sha(request)})
        try:
            raw = self.lines.get(timeout=self.commands.timeout)
        except queue.Empty as error:
            raise ExecutionError('M2 action response timeout') from error
        if raw is not None:
            self.commands.retain(phase + '-response.json', raw)
            owner.checkpoint()
        owner._effect_boundary()  # Revalidate admission after the original acknowledgement.
        require(raw is not None and len(raw) <= b02.MAX_STREAM_BYTES and raw.endswith(b'\n'),
                'Incomplete M2 action response')
        assert raw is not None
        value = profile.decode(raw)
        require(type(value) is dict and set(value) == {'phase', 'value'} and value['phase'] == phase,
                'M2 phase response differs')
        row = value['value']
        require(type(row) is dict and set(row) == {'action_index', 'result'}
            and type(row['action_index']) is int and row['action_index'] == index,
            'M2 action response index differs')
        return row


class _Session(transport._Session):
    def __init__(self, owner: CandidateM2Execution, commands: _Commands, argv: list[str]):
        require(owner.mode == 'physical', 'Fixture owners never start candidate sessions')
        self.owner, self.finished = owner, False
        owner._effect_boundary()
        argv = owner.docker + argv[1:]
        owner._retain('session-dispatch.json', encoded({'argv': argv}))
        owner.checkpoint()
        owner._effect_boundary()
        # Reuse the bounded B02 drains and finish protocol, while owning the
        # process before starting readers and closing it on partial startup.
        self.original = object.__new__(_WireSession)
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
                pass
            raise


class CandidateM2Execution(transport.CandidateStorageExecution):
    """Separate exact M2 owner, sharing only neutral bounded journal mechanics.

    The inherited owner/thread checks, raw retention, chunk validation, external
    checkpoints, effect boundary, runtime revalidation and close methods do not
    select cases or authenticate a storage-family owner. All source, review,
    binding, profile, admission, configuration and dispatch routes are M2-owned.
    """
    # The reused base defines storage-family attributes. These slots are
    # deliberately rebound by this separate exact-type constructor; inherited
    # neutral mechanics do not interpret those family-specific values.
    policy: Any
    registration: Any
    plan: Any
    review_authority: Any
    binding: Any

    def __init__(self, root: Path, store: GitStore, registration: M2Registration, policy: M2Policy, *,
                 value: profile.M2Profile, plan: review.LayoutPlan, review_authority: review.M2ReviewAuthority,
                 admission_authority: admission.ObservationAdmission, checkpoint_authority: chain.HeadAuthority,
                 delta_root: Path, cleanup_root: Path, endpoint: Any = None, mode: str = 'physical',
                 expected_checkpoint: chain.PrefixCommitment | None = None):
        require(type(registration) is M2Registration and profile.accepted_profile(value), 'Exact M2 registration/profile required')
        require(mode in ('physical', 'fixture') and type(policy) is M2Policy, 'Exact owner mode/policy required')
        require(type(review_authority) is review.M2ReviewAuthority
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
            require(self.binding == registration.binding, 'Complete registered M2 binding differs')
            self.observation_registration = observation_registration(registration)
            require(self.admission.registration == self.observation_registration, 'Admission belongs to another M2 execution')
            self.config = {'protocol': self.binding.protocol, 'mode': mode, 'root': str(self.root), 'delta_root': str(self.delta_root),
                'cleanup_root': str(self.cleanup_root), 'repository': str(store.path.resolve()), 'registration': asdict(registration),
                'profile': value.record(), 'plan': asdict(plan), 'review_sha256': self.review_sha256,
                'review_provenance': review_authority.provenance(plan),
                'source_manifest': admission.source_manifest(self.files), 'sources': self.sources,
                'runtime': self.runtime, 'policy': asdict(policy), 'source_capture_policy': SOURCE_CAPTURE_POLICY.record(),
                'journal_limits': asdict(LIMITS), 'prestart_policy': prestart.definition(),
                'endpoint': None if self.endpoint is None else asdict(self.endpoint)}
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

    def current(self, freeze: registry.CohortFreeze | None) -> None:
        self._owner()
        require(self.profile == profile_for_binding(self.binding)
            and self.plan.mapping_profile == self.profile.mapping_profile, 'Original M2 mapping profile differs')
        require(evaluator_sources() == self.sources, 'M2 evaluator changed')
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
        execution_id = 'm2-product-' + uuid.uuid4().hex
        intent = {'protocol': self.binding.protocol, 'execution_id': execution_id,
            'source_sha256': self.binding.source_sha256, 'original_binding': asdict(self.binding),
            'registration': asdict(self.observation_registration),
            'cohort_freeze': None if self._freeze is None else asdict(self._freeze),
            'container': 'gossip-' + execution_id, 'volume': 'gossip-volume-' + execution_id,
            'snapshot_protocol': b01.SNAPSHOT_PROTOCOL, 'ordered_phases': list(self.profile.phases)}
        self._retain('intent.json', encoded(intent))
        self.checkpoint()
        self.current(self._freeze)
        self._dispatch(intent)
        self.current(self._freeze)
        self.checkpoint()
        return profile.decode(self.read_authenticated('terminal.json'))

    def _dispatch(self, intent: dict[str, Any]) -> None:
        require(self.mode == 'physical', 'Fixture owners never dispatch candidate code')
        commands = _Commands(self)
        case = self.binding.case_id
        recipe = profile.recipe_for(case)
        fixtures = profile.input_fixtures(case)
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
        sandbox = DockerValidator(self.policy.image_id, {'m2_adapter.py': profile.ADAPTER}, command=('python', '-I', '-c', 'import time;time.sleep(1800)'))

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
            adapter = 'm2_adapter.py'
            expected_checks = profile.adapter_files(case)
            require(digest({'adapter_files': admission.source_manifest(expected_checks),
                'input_fixtures': fixtures, 'recipe': recipe}) == self.binding.fixture_sha256,
                'Original registered M2 helper and fixture bytes differ before dispatch')
            for helper_name, helper_raw in expected_checks.items():
                (checks / helper_name).write_bytes(helper_raw)
            b02._stage_inputs(inputs, fixtures)
            self._retain('fixture-manifest.json', encoded(b02._verify_inputs(inputs, fixtures)))
            for check_path in checks.iterdir():
                check_path.chmod(0o444)
            def verify_staging() -> dict[str, Any]:
                _verify_regular_tree(workspace, self.files)
                _verify_regular_tree(checks, expected_checks)
                b02._verify_inputs(inputs, fixtures)
                return {'source_manifest': admission.source_manifest(self.files),
                    'helper_manifest': admission.source_manifest(expected_checks),
                    'fixtures_sha256': digest(fixtures)}
            staging_proof = verify_staging()
            plan = {'workspace': str(workspace), 'checks': str(checks), 'inputs': str(inputs),
                    'source_manifest': admission.source_manifest(self.files), 'adapter_sha256': sha(profile.ADAPTER.encode()),
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
                for phase in self.profile.phases:
                    self._phase_runtime(phase + '-runtime-before')
                    self._retain(phase + '-staging-before.json', encoded(verify_staging()))
                    try:
                        session.phase(phase)
                        checked(phase + '-pause', ['docker', 'pause', container_id])
                        record = checked(phase + '-state', ['docker', 'inspect', '--format', '{{json .}}', container_id])
                        state = parsed(record)
                        require(b01._paused(state, volume, self.policy.image_id) and state.get('Id') == container_id
                            and state.get('Config', {}).get('Labels') == labels, 'Exact container snapshot ownership differs')
                        checked(phase + '-capture', ['docker', 'cp', container_id + ':/tmp', '-'], b02.MAX_CAPTURE_BYTES)
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
            raise ExecutionUnknown('Original M2 journal became uncertain; redispatch forbidden') from primary
        terminal = {'protocol': self.binding.protocol, 'intent_sha256': sha(self.read_authenticated('intent.json')),
            'execution_id': intent['execution_id'], 'case_id': case, 'family': self.binding.family,
            'source_sha256': self.binding.source_sha256, 'native_source_sha256': self.binding.native_source_sha256,
            'commands': list(commands.records), 'infrastructure': errors,
            'container_cleanup': removed['container'], 'volume_cleanup': removed['volume']}
        self._retain('terminal.json', encoded(terminal))
        if primary is not None and not isinstance(primary, Exception):
            raise primary


    def __enter__(self) -> CandidateM2Execution:
        return self
