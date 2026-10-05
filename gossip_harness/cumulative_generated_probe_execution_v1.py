"""Original-only sandbox owner for prospectively registered public probes.

No fixture dispatch route exists. The caller's admission capability must prove
unique controller slot/root enrollment and aggregate budget against originals;
this owner is not that controller. Reviews remain independently supplied. Raw
execution records are not a cold verification, selection or acceptance receipt.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import time
from typing import Any, Iterator, cast
import uuid

from . import candidate_client_process_v4 as process
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_emergency_cleanup_v1 as cleanup
from . import candidate_m2_product_observation_v1 as sqlite_observation
from . import candidate_observation_admission_v1 as admission
from . import candidate_storage_driver_v1 as storage
from . import candidate_storage_prestart_v1 as prestart
from . import candidate_storage_product_execution_v1 as transport
from . import candidate_workflow_execution_v1 as workflow
from . import cumulative_generated_probe_driver_v1 as driver
from . import cumulative_generated_probe_pipe_v1 as pipes
from . import cumulative_generated_probe_plan_v1 as plans
from . import cumulative_generated_probe_state_v1 as state
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_study_controller_v2 as study
from . import cumulative_child_deadline_v1 as child_clocks
from . import project_acceptance_registry_v1 as registry
from .sandbox import DockerValidator
from .peer_financial_authority_v5 import CumulativeAuthorityV5, EVALUATOR_CAPACITY_KEY, EVALUATOR_CAPACITY_POLICY

PROTOCOL = 'cumulative-generated-probe-execution-v1-cleanup-headroom-v5'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
NORMAL_CLEANUP_SECONDS = 60
FALLBACK_LIMITS = cleanup.CleanupLimits(total_seconds=60, request_seconds=5)
require = plans.require


def cleanup_allowance_ns() -> int:
    """Configured sequential teardown allowances; not hard OS preemption."""
    return int((pipes.CLEANUP_SECONDS + NORMAL_CLEANUP_SECONDS + FALLBACK_LIMITS.total_seconds)
               * 1_000_000_000)


@dataclass(frozen=True, slots=True)
class RuntimePolicy:
    timeout_seconds: int
    image_id: str = storage.RUNTIME_IMAGE

    def __post_init__(self) -> None:
        require(type(self.timeout_seconds) is int and 0 < self.timeout_seconds <= 30, 'bounded_probe_control_required')
        require(self.image_id == storage.RUNTIME_IMAGE, 'retained_probe_image_required')


def evaluator_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    result = {**state.evaluator_sources(), **workflow.evaluator_sources(),
        **{'gossip_harness/' + name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in
            ('cumulative_generated_probe_execution_v1.py', 'cumulative_generated_probe_pipe_v1.py',
             'candidate_m2_product_observation_v1.py')}}
    require(result['gossip_harness/cumulative_generated_probe_execution_v1.py'] == LOADED_SOURCE_SHA256,
            'loaded_probe_owner_changed')
    admission.verify_loaded_sources(result)
    return dict(sorted(result.items()))


def environment_for(plan: plans.ProbePlan, *, clock_domain: str) -> dict[str, Any]:
    require(type(plan) is plans.ProbePlan, 'exact_probe_plan_required')
    registry.identifier(clock_domain)
    policy = RuntimePolicy(plan.policy.control_seconds)
    return {'protocol': PROTOCOL, 'clock_domain': clock_domain, 'platform': platform.platform(),
        'python': platform.python_version(), 'sources': evaluator_sources(), 'runtime_policy': asdict(policy),
        'pipe': pipes.definition(pipes.PipePolicy(plan.policy.wire_limits, plan.policy.stderr_bytes, policy.timeout_seconds)),
        'prestart': prestart.definition(), 'normal_cleanup_seconds': NORMAL_CLEANUP_SECONDS,
        'fallback_cleanup': asdict(FALLBACK_LIMITS), 'local_cleanup_seconds': pipes.CLEANUP_SECONDS,
        'reserved_cleanup_allowance_ns': cleanup_allowance_ns(),
        'registration_contract': 'trusted controller authenticates unique root/slot and aggregate quotas before every effect',
        'scope': 'public generated-probe execution originals; no independent acceptance authority'}


def exec_identity(value: Any, *, container_id: str, exec_id: str, pid: int | None,
                  completed: bool = False) -> int | None:
    require(type(value) is dict and value.get('ID') == exec_id and value.get('ContainerID') == container_id,
            'exact_probe_exec_identity_required')
    config = value.get('ProcessConfig')
    require(type(config) is dict and config.get('entrypoint') == 'python'
            and config.get('arguments') == ['-I', '-B', '/checks/child_driver.py']
            and config.get('user') == '65534:65534' and config.get('privileged') is False
            and config.get('tty') is False and value.get('OpenStdin') is True,
            'exact_unprivileged_probe_command_required')
    require(pid is None or (type(pid) is int and pid > 0), 'exact_prior_probe_pid_required')
    if completed:
        require(value.get('Running') is False and type(value.get('ExitCode')) is int and value['ExitCode'] == 0
                and pid is not None, 'natural_probe_exec_completion_required')
        return pid
    actual = value.get('Pid')
    require(value.get('Running') is True and type(actual) is int and actual > 0
            and (pid is None or actual == pid), 'stable_live_probe_pid_required')
    return actual


class _Commands(transport._Commands):
    """Reuse only retained bounded control IO, not the storage owner's policy."""
    def __init__(self, owner: ProbeExecution):
        self.probe_owner = owner
        self.control_deadline: float | None = None
        super().__init__(cast(transport.CandidateStorageExecution, owner))

    def _before_spawn(self) -> None:
        self.probe_owner._check_deadline()
        require(self.control_deadline is not None and time.monotonic() < self.control_deadline,
                'probe_control_deadline')

    def _wait_timeout(self) -> float:
        assert self.control_deadline is not None
        remaining = self.control_deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired('probe control', 0)
        return remaining

    def run(self, label: str, arguments: list[str], limit: int = storage.MAX_STREAM_BYTES) -> dict[str, Any]:
        require(self.control_deadline is None, 'nested_probe_control_forbidden')
        self.control_deadline = self.probe_owner._operation_deadline(self.probe_owner.policy.timeout_seconds)
        try:
            with self.probe_owner._deadline_scope(int(self.control_deadline * 1_000_000_000)):
                result = super().run(label, arguments, limit)
                self._before_spawn()  # Also charge source validation/retention after wait.
                return result
        finally:
            self.control_deadline = None


class ProbeExecution(transport.CandidateStorageExecution):
    """Borrow exact state/journal; own only the new process and cleanup channel.

    Inherited methods are limited to raw/chunk retention, reads, checkpoints and
    owner-affinity checks. No storage profile, family dispatch or acceptance is
    reused. close() leaves the caller's state and external head open.
    """
    policy: Any
    binding: Any

    def __init__(self, probe_state: state.ProbeExecutionState, *, endpoint: process.EngineEndpoint,
                 cleanup_root: Path, executor: CumulativeAuthorityV5, study_plan: study.StudyPlan):
        require(type(probe_state) is state.ProbeExecutionState and type(endpoint) is process.EngineEndpoint,
                'exact_probe_state_and_endpoint_required')
        probe_state.current()
        self.probe_state, self.endpoint = probe_state, endpoint
        require(type(executor) is CumulativeAuthorityV5, 'actual_probe_financial_executor_required')
        self.executor = executor
        self._executor_identity = (executor.owner_id, executor.config_sha256)
        self._executor_current()
        require(type(study_plan) is study.StudyPlan, 'exact_probe_study_plan_required')
        self.study_plan = study_plan
        self._study_identity = study_plan.sha256
        self._clock_records = study.Records(executor.checkpoint_chain)
        self._clock_index = executor.terminal_config['child_index']
        self.original_child_clock = self._read_child_clock()
        self._child_clock_identity = values.digest(asdict(self.original_child_clock))
        self.policy = RuntimePolicy(probe_state.plan.policy.control_seconds)
        self.root, self.delta_root = probe_state.root, probe_state.delta_root
        self.cleanup_root = Path(cleanup_root)
        require(self.cleanup_root.is_absolute() and self.cleanup_root.resolve() == self.cleanup_root
                and not self.cleanup_root.exists(), 'fresh_canonical_cleanup_root_required')
        assert probe_state.journal is not None
        self.journal = probe_state.journal
        review_head = probe_state.review.journal.authority
        head = self.journal._chain.authority
        require(type(head) is ExternalHead and type(review_head) is ExternalHead, 'physical_external_heads_required')
        assert isinstance(head, ExternalHead) and isinstance(review_head, ExternalHead)
        self.checkpoint_authority = head
        self._physical_head = head
        roots = (self.root, self.delta_root, head.root, probe_state.store.path,
                 probe_state.review.journal.raw_root, probe_state.review.journal.delta_root, review_head.root)
        require(all(not self.cleanup_root.is_relative_to(p) and not p.is_relative_to(self.cleanup_root) for p in roots),
                'probe_cleanup_origin_overlap')
        self._pid, self._thread = probe_state._pid, probe_state._thread
        self.closed, self.mode = False, 'physical'
        self.binding = probe_state.binding
        self.runtime = json.loads(probe_state.runtime_raw)
        self.environment = json.loads(probe_state.environment_raw)
        require(type(self.environment.get('clock_domain')) is str
                and values.canonical(self.environment) == values.canonical(environment_for(probe_state.plan,
                    clock_domain=self.environment['clock_domain'])), 'physical_probe_environment_differs')
        require(self.runtime.get('endpoint') == asdict(endpoint) and self.runtime.get('image_id') == self.policy.image_id,
                'registered_probe_runtime_endpoint_differs')
        self.sources = evaluator_sources()
        self.docker = ['docker', '--host', 'unix://' + endpoint.socket_path]
        self._cleanup: cleanup.CleanupChannel | None = None
        self._cleanup_phase = False
        self._cleanup_deadline: float | None = None
        self._active_deadline_ns: int | None = None
        self.cleanup_result: Any = None
        self._pipe: pipes.ProbePipe | None = None
        self._child: subprocess.Popen[bytes] | None = None
        self._exec_id: str | None = None
        self._exec_pid: int | None = None
        self._created: dict[str, Any] | None = None
        self._running: dict[str, Any] | None = None
        self._attempted = False
        self._capacity_active = False
        self._capacity_dispatched = False
        self._child_clock_current()
        self.endpoint.validate()

    def _executor_current(self) -> None:
        executor = self.executor
        require(type(executor) is CumulativeAuthorityV5
                and (executor.owner_id, executor.config_sha256) == self._executor_identity,
                'original_probe_executor_changed')
        executor._guard_evidence()
        subject = self.probe_state.plan.target.subject
        require(executor.contract['cohort_id'] == subject.cohort_id
                and executor.contract['execution_contract_sha256'] == subject.execution_contract_sha256
                and executor.terminal_config['child']['trajectory'] == subject.trajectory_id,
                'probe_executor_cohort_or_trajectory_differs')
        require(values.exact(executor.permit['execution_design'].get('runtime', {}).get(EVALUATOR_CAPACITY_KEY),
                             EVALUATOR_CAPACITY_POLICY), 'probe_shared_executor_policy_required')

    def _read_child_clock(self) -> child_clocks.ChildClock:
        require(type(self.study_plan) is study.StudyPlan and self.study_plan.sha256 == self._study_identity
                == self.executor.contract['execution_contract_sha256'], 'probe_original_study_changed')
        require(self._clock_records.chain is self.executor.checkpoint_chain
                and values.exact(self.study_plan.roster.record(), self.executor.terminal_roster.record()),
                'probe_original_controller_or_roster_changed')
        require(values.exact(self.study_plan.runtime, self.executor.permit['execution_design'].get('runtime')),
                'probe_financial_runtime_differs_from_study')
        self.study_plan.verify_sources(Path(__file__).resolve().parents[1])
        return child_clocks.read(self._clock_records, self.study_plan, self._clock_index, active=True)

    def _child_clock_current(self) -> None:
        original = self._read_child_clock()
        require(original == self.original_child_clock
                and values.digest(asdict(original)) == self._child_clock_identity,
                'probe_original_child_clock_changed')
        window = self.binding.window
        require(self.environment['clock_domain'] == original.clock_domain
                and original.started_ns <= window.started_ns < window.deadline_ns <= original.deadline_ns,
                'probe_window_outside_original_child')
        require(window.deadline_ns + cleanup_allowance_ns() <= original.deadline_ns,
                'probe_window_leaves_no_original_cleanup_allowance')
        require(not original.expired(time.time()), 'probe_original_child_deadline_expired')

    def _local_cleanup_complete(self) -> bool:
        if self._pipe is not None and (not self._pipe.closed or self._pipe.cleanup_errors):
            return False
        child = self._child
        return child is None or (child.poll() is not None and all(
            stream is None or stream.closed for stream in (child.stdin, child.stdout, child.stderr)))

    def _check_deadline(self) -> None:
        require(self._active_deadline_ns is None or time.monotonic_ns() < self._active_deadline_ns,
                'probe_nested_control_deadline')
        if self._cleanup_phase:
            require(self._cleanup_deadline is not None and time.monotonic() < self._cleanup_deadline,
                    'probe_cleanup_deadline')
        else:
            require(values.digest(asdict(self.original_child_clock)) == self._child_clock_identity,
                    'probe_original_child_clock_changed')
            require(not self.original_child_clock.expired(time.time()), 'probe_original_child_deadline_expired')
            self.probe_state._window()

    @contextmanager
    def _deadline_scope(self, deadline_ns: int) -> Iterator[None]:
        """All nested control/source operations inherit the earliest live bound."""
        self._owner()
        require(type(deadline_ns) is int and 0 < deadline_ns <= 2**63-1,
                'exact_probe_nested_deadline_required')
        previous = self._active_deadline_ns
        self._active_deadline_ns = min(previous, deadline_ns) if previous is not None else deadline_ns
        try:
            self._check_deadline()
            yield
            self._check_deadline()
        finally:
            self._active_deadline_ns = previous

    def _source_deadline_ns(self) -> int:
        return min(self.binding.window.deadline_ns, self._active_deadline_ns
                   if self._active_deadline_ns is not None else self.binding.window.deadline_ns)

    def _operation_deadline(self, seconds: float) -> float:
        deadline = time.monotonic() + seconds
        cap = self._cleanup_deadline if self._cleanup_phase else self.binding.window.deadline_ns / 1_000_000_000
        assert cap is not None
        if self._active_deadline_ns is not None:
            cap = min(cap, self._active_deadline_ns / 1_000_000_000)
        return min(deadline, cap)

    def _effect_boundary(self) -> None:
        self._owner(); self._check_deadline(); self.checkpoint(); self.endpoint.validate()
        require(evaluator_sources() == self.sources, 'probe_execution_sources_changed')
        require(self.policy == RuntimePolicy(self.probe_state.plan.policy.control_seconds)
                and values.canonical(self.runtime) == self.probe_state.runtime_raw
                and values.canonical(self.environment) == self.probe_state.environment_raw, 'probe_owner_binding_changed')
        if not self._cleanup_phase:
            self._executor_current()
            self._child_clock_current()
            self.probe_state.current(deadline_ns=self._source_deadline_ns())
        self._check_deadline()

    def _runtime(self, label: str) -> None:
        self._effect_boundary()
        actual = workflow._runtime_identity(self.endpoint, self.policy.image_id,
            deadline=self._operation_deadline(min(15, self.policy.timeout_seconds)), retain=self._retain, label=label)
        require(actual == self.runtime, 'probe_runtime_changed')
        self._check_deadline()
        self._retain(label + '-verified.json', values.canonical({'runtime_sha256': values.digest(actual)}))

    def _identity(self, container_id: str, label: str, *, completed: bool = False) -> None:
        self._effect_boundary(); deadline = self._operation_deadline(self.policy.timeout_seconds)
        observed = process._json_control(self.endpoint, '/containers/' + container_id + '/json',
            deadline=deadline, retain=self._retain, label=label + '-container')
        require(self._created is not None, 'created_probe_container_unavailable')
        assert self._created is not None
        prestart.validate_continuity(self._created, observed, self.runtime, previous_running=self._running)
        self._running = observed
        if self._exec_id is None:
            ids = observed.get('ExecIDs')
            require(type(ids) is list and len(ids) == 1, 'unambiguous_probe_exec_required')
            assert isinstance(ids, list)
            registry.sha256(ids[0]); self._exec_id = ids[0]
        assert self._exec_id is not None
        value = process._json_control(self.endpoint, '/exec/' + self._exec_id + '/json',
            deadline=deadline, retain=self._retain, label=label + '-exec')
        self._exec_pid = exec_identity(value, container_id=container_id, exec_id=self._exec_id,
                                      pid=self._exec_pid, completed=completed)
        self._effect_boundary()
        self._retain(label + '-identity-verified.json', values.canonical({'exec_id': self._exec_id,
            'pid': self._exec_pid, 'completed': completed, 'value_sha256': values.digest(value)}))
        self.checkpoint(); self._check_deadline()

    def execute_once(self) -> dict[str, Any]:
        self._owner(); require(not self._attempted, 'probe_owner_is_one_shot')
        self._attempted = True
        self._effect_boundary()
        self._executor_current()
        with self.executor.evaluation_slot(deadline_ns=self.binding.window.deadline_ns) as slot:
            self._capacity_active = True
            try:
                self._executor_current()
                self._effect_boundary()  # Waiting cannot renew the registered window.
                result = self._execute_admitted()
                if (result['container_cleanup'] is True and result['volume_cleanup'] is True
                        and result['local_cleanup'] is True):
                    slot.confirm_cleanup()
                return result
            finally:
                self._capacity_active = False
                if not self._capacity_dispatched:
                    slot.confirm_cleanup()  # No probe setup or Engine effect was invoked.

    def _execute_admitted(self) -> dict[str, Any]:
        require(self._capacity_active, 'shared_probe_executor_slot_required')
        require(values.canonical(self.environment) == values.canonical(environment_for(self.probe_state.plan,
                clock_domain=self.environment['clock_domain'])), 'physical_probe_environment_changed')
        self.probe_state.begin()
        execution_id = 'probe-' + uuid.uuid4().hex
        intent = {'protocol': PROTOCOL, 'execution_id': execution_id, 'container': 'gossip-' + execution_id,
            'volume': 'gossip-volume-' + execution_id, 'binding_sha256': values.digest(asdict(self.binding)),
            'state_intent_sha256': transport.sha(self.read_authenticated('intent.json')),
            'cleanup_root': str(self.cleanup_root), 'environment': self.environment}
        self._retain('physical-intent.json', values.canonical(intent)); self._effect_boundary()
        self._capacity_dispatched = True
        return self._dispatch_probe(intent)

    def _dispatch_probe(self, intent: dict[str, Any]) -> dict[str, Any]:
        require(self._capacity_active, 'shared_probe_executor_slot_required')
        commands = _Commands(self)
        files = plans.verify_current_source(self.probe_state.store, self.probe_state.plan,
            deadline_ns=self._source_deadline_ns())
        record = self.probe_state.plan.record()
        helpers = driver.adapter_files(record['probe'], released_requirements=tuple(record['released_requirements']))
        require(admission.source_manifest(helpers) == record['helper_manifest'], 'probe_helper_manifest_differs')
        name, volume = intent['container'], intent['volume']
        labels = {'gossip.execution': intent['execution_id'], 'gossip.source': self.binding.source_sha256,
                  'gossip.fixture': values.digest(admission.source_manifest(helpers))}
        sandbox = DockerValidator(self.policy.image_id, {k: v.decode('utf-8') for k, v in helpers.items()},
                                  command=prestart.COMMAND)
        container_id: str | None = None
        claims: dict[str, str] = {}
        attempted = {'container': False, 'volume': False}
        removed = {'container': False, 'volume': False}
        errors: list[str] = []
        primary: BaseException | None = None
        pipe_result: dict[str, Any] | None = None
        qualified = False

        def checked(label: str, argv: list[str], limit: int = storage.MAX_STREAM_BYTES) -> dict[str, Any]:
            result = commands.run(label, argv, limit)
            require(storage._clean(result), label + ': incomplete_probe_control')
            return result

        def parsed(result: dict[str, Any]) -> Any:
            return process.strict_json_loads(commands.raw(result))

        def ordinary_cleanup() -> None:
            self._cleanup_phase = True
            self._cleanup_deadline = time.monotonic() + NORMAL_CLEANUP_SECONDS
            assert self._cleanup is not None
            if container_id is not None:
                self._cleanup.note_normal_removal(claims['container'])
                checked('container-remove', ['docker', 'rm', '--force', container_id])
                after = checked('container-after', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])
                removed['container'] = not commands.raw(after).strip()
                require(removed['container'], 'probe_container_absence_unproven')
                self._cleanup.confirm_normal_removal(claims['container'], remove_record='container-remove.json', absence_record='container-after.json')
            elif not attempted['container']:
                removed['container'] = True
            require(removed['container'], 'probe_container_create_outcome_unknown')
            if attempted['volume'] and 'volume' in claims:
                current = checked('volume-cleanup-inspect', ['docker', 'volume', 'inspect', '--format', '{{json .}}', volume])
                require(storage._volume_valid(parsed(current), volume, intent['execution_id']), 'probe_cleanup_volume_identity_differs')
                self._cleanup.note_normal_removal(claims['volume'])
                checked('volume-remove', ['docker', 'volume', 'rm', volume])
                after = checked('volume-after', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$'])
                removed['volume'] = not commands.raw(after).strip()
                require(removed['volume'], 'probe_volume_absence_unproven')
                self._cleanup.confirm_normal_removal(claims['volume'], remove_record='volume-remove.json', absence_record='volume-after.json')
            elif not attempted['volume']:
                removed['volume'] = True

        with tempfile.TemporaryDirectory(prefix='generated-probe-sandbox-') as directory:
            base = Path(directory).resolve(); workspace, checks = base/'source', base/'checks'
            for root, content in ((workspace, files), (checks, helpers)):
                root.mkdir(mode=0o755)
                for relative, raw in content.items():
                    path = root/relative; path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    path.write_bytes(raw); path.chmod(0o444)

            def verify_staging() -> dict[str, Any]:
                transport._verify_regular_tree(workspace, files); transport._verify_regular_tree(checks, helpers)
                return {'source_manifest': admission.source_manifest(files), 'helper_manifest': admission.source_manifest(helpers)}

            self._retain('staging.json', values.canonical({'workspace': str(workspace), 'checks': str(checks), 'proof': verify_staging()}))
            assert self.journal is not None
            self._cleanup = cleanup.CleanupChannel.create(self.cleanup_root, journal=self.journal, endpoint=self.endpoint,
                runtime=self.runtime, source_sha256=self.binding.source_sha256, fixture_sha256=labels['gossip.fixture'],
                execution_id=intent['execution_id'], image_id=self.policy.image_id, candidate_mount_roots=(workspace, checks),
                journal_roots=(self.root, self.delta_root, self._physical_head.root), limits=FALLBACK_LIMITS)
            try:
                self._runtime('runtime')
                for kind, argv in (('volume', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$']),
                    ('container', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])):
                    require(not commands.raw(checked(kind+'-before', argv)).strip(), 'probe_owned_name_exists')
                claims['volume'] = self._cleanup.claim_volume(name=volume,
                    labels={'gossip.execution': intent['execution_id'], 'gossip.snapshot': storage.SNAPSHOT_PROTOCOL},
                    options=storage.VOLUME_OPTIONS, preabsence_record='volume-before.json')
                attempted['volume'] = True
                argv = ['docker', 'volume', 'create', '--driver', 'local', '--label', 'gossip.execution='+intent['execution_id'],
                        '--label', 'gossip.snapshot='+storage.SNAPSHOT_PROTOCOL]
                for k, v in storage.VOLUME_OPTIONS.items(): argv.extend(('--opt', k+'='+v))
                argv.append(volume)
                require(commands.raw(checked('volume-create', argv)).strip() == volume.encode(), 'probe_created_volume_differs')
                created_volume = checked('volume-created', ['docker', 'volume', 'inspect', '--format', '{{json .}}', volume])
                require(storage._volume_valid(parsed(created_volume), volume, intent['execution_id']), 'probe_volume_ownership_differs')
                self._cleanup.confirm_volume(claims['volume'], inspection_record='volume-created.json')
                argv = storage._start_arguments(sandbox, name, workspace, checks, volume)
                argv[1] = 'create'; argv.remove('--detach')
                index = argv.index('--entrypoint')
                for k, v in labels.items(): argv[index:index] = ['--label', k+'='+v]; index += 2
                claims['container'] = self._cleanup.claim_container(name=name, labels=labels,
                    argv=sandbox.command, preabsence_record='container-before.json')
                attempted['container'] = True
                container_id = commands.raw(checked('container-create', argv)).strip().decode('ascii'); registry.sha256(container_id)
                self._cleanup.confirm_container(claims['container'], create_record='container-create.json')
                before = checked('container-prestart', ['docker', 'inspect', '--format', '{{json .}}', container_id], process.CONTROL_LIMIT)
                self._created = parsed(before)
                proof = prestart.proof_for(commands.raw(before), container_id=container_id, name=name,
                    image_id=self.policy.image_id, volume=volume, labels=labels, mounts={'/workspace': str(workspace), '/checks': str(checks)},
                    runtime=self.runtime, runtime_originals={n: self.read_authenticated(n) for n in prestart.RUNTIME_ORIGINAL_NAMES})
                self._retain(prestart.PROOF_FILE, values.canonical(proof)); self._effect_boundary()
                checked('container-start', ['docker', 'start', container_id])
                session_argv = self.docker + ['exec', '--interactive', '--user', '65534:65534', container_id,
                                              'python', '-I', '-B', '/checks/child_driver.py']
                self._retain('session-dispatch.json', values.canonical({'argv': session_argv})); self._effect_boundary()
                self._child = subprocess.Popen(session_argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, env=DockerValidator._environment())
                self._pipe = pipes.ProbePipe(self._child,
                    pipes.PipePolicy(self.probe_state.plan.policy.wire_limits, self.probe_state.plan.policy.stderr_bytes, self.policy.timeout_seconds),
                    deadline_ns=self.binding.window.deadline_ns)

                def retain_pipe(label: str, raw: bytes) -> None:
                    self._retain_blob(label, raw)
                    if label.startswith('probe-frame-'):
                        slot = label[len('probe-frame-'):-len('.bin')]
                        self._runtime(slot+'-runtime')
                        self._identity(container_id, slot)
                        self._retain(slot+'-staging.json', values.canonical(verify_staging()))
                        self._effect_boundary()

                def capture_job() -> Any:
                    layout = self.probe_state.plan.layout
                    require(type(layout) is plans.CaptureLayout, 'reviewed_probe_capture_layout_required')
                    self._runtime('capture-runtime-before'); self._identity(container_id, 'capture-before')
                    try:
                        checked('capture-pause', ['docker', 'pause', container_id])
                        paused = parsed(checked('capture-state', ['docker', 'inspect', '--format', '{{json .}}', container_id]))
                        require(storage._paused(paused, volume, self.policy.image_id), 'paused_probe_capture_required')
                        assert self._created is not None
                        prestart.validate_continuity(self._created, paused, self.runtime, previous_running=self._running)
                        captured = checked('capture-tar', ['docker', 'cp', container_id+':/tmp', '-'], storage.MAX_CAPTURE_BYTES)
                        captured_files = storage.parse_capture(commands.raw(captured))
                        value = sqlite_observation.sqlite_job_value(captured_files, layout, values.JOB)
                    finally:
                        checked('capture-unpause', ['docker', 'unpause', container_id])
                    self._runtime('capture-runtime-after'); self._identity(container_id, 'capture-after')
                    self._retain('capture-staging.json', values.canonical(verify_staging()))
                    self._retain('captured-job.json', values.canonical(value)); self._effect_boundary()
                    return value

                pipe_result = self._pipe.exchange(record['probe'], released_requirements=tuple(record['released_requirements']),
                    before_effect=self._effect_boundary, retain=retain_pipe, capture_job=capture_job,
                    deadline_scope=self._deadline_scope)
                require(pipe_result['mechanics_complete'], 'probe_pipe_mechanics_incomplete')
                self._identity(container_id, 'session-final', completed=True); self._runtime('runtime-final')
                self._retain('staging-final.json', values.canonical(verify_staging())); self._effect_boundary()
                qualified = True
            except BaseException as error:
                primary = error; errors.append(type(error).__name__+':'+str(error)[:300])
            finally:
                try:
                    if self._pipe is not None:
                        self._pipe.close()
                    elif self._child is not None:
                        transport._stop_local_process(self._child, [])
                except BaseException as error:
                    errors.append('local_cleanup:'+type(error).__name__)
                try:
                    ordinary_cleanup()
                except BaseException as error:
                    errors.append('cleanup:'+type(error).__name__+':'+str(error)[:200])
                    try:
                        self.cleanup_result = self._cleanup.run(reason=str(primary or error)[:300])
                    except BaseException as fallback:
                        errors.append('fallback:'+type(fallback).__name__)
                self._cleanup_phase = False
        assert self.journal is not None
        if self.journal.uncertain:
            raise transport.ExecutionUnknown('Probe journal uncertain; no redispatch or observation') from primary
        try:
            self._effect_boundary()
        except BaseException as error:
            errors.append('final_admission:'+type(error).__name__+':'+str(error)[:200]); qualified = False
        terminal = {'protocol': PROTOCOL, 'execution_id': intent['execution_id'], 'pipe_result': pipe_result,
            'qualified_execution_originals': qualified and not errors and all(removed.values()) and self._local_cleanup_complete(),
            'container_cleanup': removed['container'], 'volume_cleanup': removed['volume'], 'infrastructure': errors,
            'local_cleanup': self._local_cleanup_complete(),
            'acceptance_authority': False, 'cold_reconstruction_supplied': False}
        self._retain('physical-terminal.json', values.canonical(terminal)); self.checkpoint()
        if primary is not None and not isinstance(primary, Exception): raise primary
        return terminal

    def close(self) -> None:
        if getattr(self, 'closed', True): return
        self._owner()
        try:
            if self._pipe is not None: self._pipe.close()
            elif self._child is not None: transport._stop_local_process(self._child, [])
        finally:
            if self._cleanup is not None: self._cleanup.close()
            self.closed = True
