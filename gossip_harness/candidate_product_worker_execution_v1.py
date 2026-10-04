"""Owned physical worker histories, distinct from the eight frozen process cases.

No fixture-mode or fabricated process-result entry point exists. Public release
observations require prospective admission; independent-purpose wiring remains
closed until the new selector family is globally frozen by the parent workflow.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from typing import Any

from . import candidate_product_process_execution_v1 as base
from . import candidate_product_worker_cases_v1 as cases
from . import candidate_product_worker_hooks_v1 as hooks
from . import candidate_client_execution_v4 as finite
from . import candidate_client_process_v4 as engine
from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_checkpoint_head_v1 as head
from . import candidate_execution_journal_v1 as owner_journal
from . import candidate_emergency_cleanup_v1 as emergency
from . import candidate_observation_admission_v1 as admission
from . import candidate_retention_process_v1 as retained_process
from . import project_acceptance_registry_v1 as registry
from .candidate_release_execution_v2 import capture_git_source
from .candidate_observation_admission_v1 import source_manifest, source_sha256
from .gitstore import GitStore
from .sandbox import DockerValidator

PROTOCOL = "candidate-product-worker-execution-v1"
PROFILE_PROTOCOL = "candidate-product-worker-profile-v1"
SNAPSHOT_PROTOCOL = "docker-owned-worker-crash-held-tmpfs-v1"
ROLE_POLICY = "candidate-worker-cli-source-and-input-mounts-keeper-source-free-v1"
CONTROL_LIMIT = 8 * 1024 * 1024
CLEANUP_SECONDS = 300
MAX_RECORD_BYTES = 32 * 1024 * 1024
VOLUME_OPTIONS = dict(base.VOLUME_OPTIONS)
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_NAME = re.compile(r"[a-z][a-z0-9.-]{0,100}\Z")
RoleSpec = base.RoleSpec
create_argv = base.create_argv
validate_role = base.validate_role
validate_state = base.validate_state
role_identity_comparison = base.role_identity_comparison
ExecutionError = base.ExecutionError
ExecutionUnknown = base.ExecutionUnknown
require = base.require
encoded = cases.encoded
sha256 = cases.sha


def digest(value: Any) -> str:
    return sha256(encoded(value))


def evaluator_sources() -> dict[str, str]:
    result = dict(base.evaluator_sources())
    for name in ("candidate_product_worker_cases_v1.py", "candidate_product_worker_hooks_v1.py",
                 "candidate_product_worker_execution_v1.py", "candidate_product_worker_observation_v1.py",
                 "candidate_product_worker_fixture_v1.py"):
        result["gossip_harness/" + name] = sha256(Path(__file__).with_name(name).read_bytes())
    public_hook = Path(__file__).resolve().parents[1] / "docs/candidate-product-worker-hooks-v1.json"
    raw = public_hook.read_bytes()
    require(encoded(engine.strict_json_loads(raw)) == encoded(hooks.interface()), "Frozen public hook document differs from execution interface")
    result["docs/candidate-product-worker-hooks-v1.json"] = sha256(raw)
    return result


@dataclass(frozen=True)
class WorkerPolicy:
    image_id: str
    transport_timeout_seconds: float = 15
    cli_timeout_seconds: float = 30
    hook_timeout_seconds: float = 10
    lifetime_seconds: float = 900
    stream_limit_bytes: int = 1024 * 1024
    stop_timeout_seconds: int = 5
    seed: int = 0

    def __post_init__(self) -> None:
        engine.ProcessPolicy(self.image_id, timeout_seconds=self.cli_timeout_seconds,
            transport_timeout_seconds=self.transport_timeout_seconds, stream_limit_bytes=self.stream_limit_bytes,
            frame_limit_bytes=self.stream_limit_bytes)
        require(type(self.hook_timeout_seconds) in (int, float) and math.isfinite(self.hook_timeout_seconds)
                and 0 < self.hook_timeout_seconds <= 15, "Bounded hook deadline required")
        require(type(self.lifetime_seconds) in (int, float) and math.isfinite(self.lifetime_seconds)
                and 600 <= self.lifetime_seconds <= 1800 and self.seed == 0 and type(self.seed) is int,
                "Fixed worker history resource policy required")
        require(self.stop_timeout_seconds == 5, "Fixed keeper stop bound required")

    @property
    def process(self) -> engine.ProcessPolicy:
        return engine.ProcessPolicy(self.image_id, timeout_seconds=self.cli_timeout_seconds,
            transport_timeout_seconds=self.transport_timeout_seconds, stream_limit_bytes=self.stream_limit_bytes,
            frame_limit_bytes=self.stream_limit_bytes)


@dataclass(frozen=True)
class WorkerProfile:
    case: cases.WorkerCase

    def __post_init__(self) -> None:
        require(type(self.case) is cases.WorkerCase, "Closed worker case required")
        self.case.check_current()

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return (*self.case.facet_ids, self.case.identifier + ":mechanics")

    def record(self) -> dict[str, Any]:
        return {"protocol": PROFILE_PROTOCOL, "definition": self.case.record,
            "original_definition_purpose": cases.ORIGINAL_PURPOSE,
            "ordered_case_ids": self.ordered_case_ids, "hook_interface": hooks.interface(),
            "whole_product_acceptance": False, "held_out_claim": False,
            "aggregation": "retain-known-discrepancy-separately-from-unavailable-facets",
            "independent_purpose_integration": "not enabled by this draft"}

    @property
    def sha256(self) -> str:
        return digest(self.record())


@dataclass(frozen=True)
class WorkerBinding:
    source_sha256: str
    requirements_sha256: str
    recipe_sha256: str
    fixture_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    profile_sha256: str
    helper_sha256: str
    purpose: str = "public_release"
    milestone: str = "M4"
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        require(all(type(value) is str and _SHA.fullmatch(value) for key, value in asdict(self).items()
                    if key.endswith("_sha256")), "Exact binding hashes required")
        require(self.requirements_sha256 == cases.CONTRACT_SHA256 and self.protocol == PROTOCOL
                and self.milestone == "M4" and self.purpose == "public_release",
                "This new family currently permits prospectively admitted public development only")


def binding_for(files: dict[str, bytes], profile: WorkerProfile, policy: WorkerPolicy,
                runtime: dict[str, Any]) -> WorkerBinding:
    profile.__post_init__()
    return WorkerBinding(source_sha256(files), cases.CONTRACT_SHA256, profile.case.sha256,
        digest(source_manifest(profile.case.inputs())), digest(evaluator_sources()), digest(runtime),
        digest({"environment": DockerValidator._environment(), "role_policy": ROLE_POLICY,
                "volume_options": VOLUME_OPTIONS}), digest({"policy": asdict(policy), "journal": asdict(chain.Limits()),
                "control_bytes": CONTROL_LIMIT, "max_unretired_containers": 3}),
        digest({"seed": 0, "meaning": "fixed public input"}), profile.sha256, digest(hooks.interface()))


def observation_registration_for(binding: WorkerBinding, profile: WorkerProfile, policy: WorkerPolicy, *,
        subject: registry.Subject, gate_id: str, commit_oid: str, tree_oid: str, repetition_id: str,
        cohort_trajectory_ids: tuple[str, ...]) -> admission.ObservationRegistration:
    require(type(binding) is WorkerBinding and type(profile) is WorkerProfile and type(subject) is registry.Subject
            and subject.source_sha256 == binding.source_sha256 and subject.requirements_sha256 == cases.CONTRACT_SHA256
            and subject.milestone == "M4" and binding.profile_sha256 == profile.sha256,
            "Exact public subject/profile binding required")
    gate_binding = registry.Binding(subject, digest({"profile": profile.record(), "case": profile.case.sha256}),
        binding.evaluator_sha256, policy.image_id.removeprefix("sha256:"),
        digest({"environment": binding.environment_sha256, "runtime": binding.runtime_sha256}),
        binding.limits_sha256, binding.seed_sha256, PROTOCOL, binding.purpose)
    gate = registry.Gate(gate_id, tuple(profile.case.record["requirements"]), profile.ordered_case_ids, gate_binding)
    return admission.ObservationRegistration(gate, commit_oid, tree_oid, repetition_id, cohort_trajectory_ids,
        profile.case.sha256, profile.sha256, cases.ORIGINAL_PURPOSE, admission.binding_sha256(binding, gate=gate))


@dataclass(frozen=True)
class WorkerRegistration:
    binding: WorkerBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    observation: admission.ObservationRegistration

    def __post_init__(self) -> None:
        require(type(self.binding) is WorkerBinding and type(self.observation) is admission.ObservationRegistration
                and all(type(x) is str and re.fullmatch(r"[0-9a-f]{40}", x) for x in (self.commit_oid, self.tree_oid))
                and _NAME.fullmatch(self.repetition_id) is not None, "Typed immutable Git registration required")


class _LiveProcess:
    """Split start/finish attach owner. No process result can be supplied by a caller."""
    def __init__(self, owner: WorkerExecution, label: str, spec: RoleSpec, created: dict[str, Any]):
        self.owner, self.label, self.spec, self.created = owner, label, spec, created
        self.decoder = engine.MultiplexDecoder(owner.policy.process)
        self.errors: list[str] = []
        self.started = False
        self.running: dict[str, Any] | None = None
        self.eof_after_start: bool | None = None
        self.wire: Any = None
        self.reader: threading.Thread | None = None
        self.closed = False
        self.hook_mode = "observe"
        self.start_before_ns: int | None = None
        self.release_label: str | None = None

    def save(self, name: str, raw: bytes) -> None:
        self.owner._retain(self.label + "-" + name, raw)

    def start(self) -> dict[str, Any]:
        owner = self.owner
        prefix = "/containers/" + self.created["Id"]
        before = owner._inspect(self.label + "-prestart", self.created["Id"])
        validate_state(before, "created")
        owner._compare(self.label + "-prestart", self.created, before, self.spec, "created-to-prestart")
        request = engine._request("POST", prefix + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True)
        self.save("attach-request.bin", request)
        self.wire = engine._Wire(owner.endpoint, owner._deadline_after(owner.policy.transport_timeout_seconds),
            engine.HEADER_LIMIT + 2 * owner.policy.stream_limit_bytes + engine.FRAME_COUNT_LIMIT * 8)
        self.wire.send(request)
        status, headers = self.wire.headers()
        require(status == 101 and headers.get("connection", "").lower() == "upgrade"
                and headers.get("upgrade", "").lower() == "tcp"
                and headers.get("content-type") in ("application/vnd.docker.raw-stream", "application/vnd.docker.multiplexed-stream")
                and "content-length" not in headers and "transfer-encoding" not in headers and not self.wire.buffer,
                "Complete idle attach upgrade required")
        self.wire.socket.settimeout(0)
        try:
            pending = self.wire.socket.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            pass
        else:
            self.save("prestart-pending.bin", pending)
            raise ExecutionUnknown("Attach was not idle/live before start")
        finally:
            self.wire._timeout()
        self.wire.deadline = owner._work_deadline

        def drain() -> None:
            try:
                while True:
                    if self.wire.buffer:
                        chunk = bytes(self.wire.buffer)
                        self.wire.buffer.clear()
                        self.decoder.feed(chunk)
                    if self.wire.eof:
                        self.eof_after_start = self.started
                        require(self.eof_after_start is True, "Attach EOF before confirmed start")
                        self.decoder.eof()
                        return
                    self.wire.receive()
            except (OSError, ValueError) as error:
                self.errors.append(type(error).__name__ + ":" + str(error)[:512])

        self.reader = threading.Thread(target=drain, daemon=True)
        self.reader.start()
        require(not self.errors and not self.decoder.complete, "Attach failed before start")

        def observe(status_code: int) -> None:
            if status_code == 204:
                self.started = True

        self.start_before_ns = time.monotonic_ns()
        self.save("instrumentation-start.json", encoded({"protocol": "worker-hook-host-bound-v1",
            "clock": "host-monotonic-ns", "container_id": self.created["Id"],
            "start_before_ns": self.start_before_ns, "maximum_hold_seconds": hooks.MAX_HOLD_SECONDS,
            "hook_mode": self.hook_mode}))
        code, body = engine._control(owner.endpoint, "POST", prefix + "/start",
            deadline=owner._deadline_after(owner.policy.transport_timeout_seconds), retain=self.save,
            label="start", on_response=observe)
        self.save("start-completion.json", encoded({"status": code, "body_sha256": sha256(body),
            "container_id": self.created["Id"], "framing_complete": True, "eof_observed": True}))
        require(code == 204, "Worker start rejected")
        self.running = owner._inspect(self.label + "-running", self.created["Id"])
        validate_state(self.running, "running")
        owner._compare(self.label + "-startup", self.created, self.running, self.spec, "created-to-running")
        self.save("running.json", encoded(self.running))
        return self.running

    def finish(self, *, kill: bool, row: dict[str, Any]) -> dict[str, Any]:
        owner, cid = self.owner, self.created["Id"]
        require(self.running is not None and self.started, "Live source-bound worker required")
        assert self.running is not None
        if kill:
            owner._boundary(self.label + "-prekill", self.running, self.spec)
            self.save("kill-intent.json", encoded({"controller_action": "abrupt-SIGKILL", "container_id": cid,
                "graceful_stop": False, "running_sha256": digest(self.running)}))
            code, body = engine._control(owner.endpoint, "POST", "/containers/" + cid + "/kill?signal=SIGKILL",
                deadline=owner._deadline_after(owner.policy.transport_timeout_seconds), retain=self.save, label="kill")
            self.save("kill-completion.json", encoded({"status": code, "body_sha256": sha256(body),
                "framing_complete": True, "eof_observed": True, "container_id": cid}))
            require(code == 204, "SIGKILL acknowledgement unavailable")
        waited = engine._json_control(owner.endpoint, "/containers/" + cid + "/wait?condition=not-running",
            deadline=owner._deadline_after(owner.policy.cli_timeout_seconds), retain=self.save, label="wait", method="POST")
        final = owner._inspect(self.label + "-final", cid)
        validate_state(final, "exited")
        comparison = owner._compare(self.label + "-final", self.running, final, self.spec, "running-to-exited")
        completion = engine.completion_evidence(waited, final, started=True, killed=kill,
                                                identity_ok=comparison["matches"] is True)
        # For an abrupt event, separate causality (204 kill) from natural exit.
        abrupt = (kill and waited.get("StatusCode") == final["State"].get("ExitCode") == 137
                  and not waited.get("Error") and final["State"].get("OOMKilled") is False)
        exit_facts = {"protocol": PROTOCOL, "container_id": cid, "completion": completion,
                      "abrupt_death": abrupt, "exit_code": final["State"]["ExitCode"]}
        self.save("exit.json", encoded(exit_facts))
        row.update(exit_facts)
        require(abrupt if kill else completion["natural"] is True, "Worker completion cause unavailable")
        assert self.reader is not None
        self.reader.join(timeout=owner._remaining(owner.policy.transport_timeout_seconds))
        require(not self.reader.is_alive() and not self.errors and self.decoder.complete and self.eof_after_start is True,
                "Complete original attach EOF unavailable")
        self.save("attach-response.bin", bytes(self.wire.raw))
        result: dict[str, Any] = {**exit_facts,
            "capture_complete": True, "attach_eof_after_start_confirmation": self.eof_after_start,
            "capture": {}}
        for name, data in self.decoder.streams.items():
            raw = bytes(data)
            self.save(name + ".bin", raw)
            result["capture"][name] = {"path": self.label + "-" + name + ".bin", "bytes": len(raw),
                "sha256": sha256(raw), "observed_bytes": self.decoder.counts[name], "truncated": False}
        self.save("completion.json", encoded(result))
        self.close()
        return result

    def close(self) -> None:
        if self.closed:
            return
        errors: list[BaseException] = []
        try:
            if self.wire is not None:
                try:
                    self.wire.socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                except BaseException as error:
                    errors.append(error)
                if self.reader is not None and self.reader.ident is not None:
                    try:
                        self.reader.join(timeout=self.owner.policy.transport_timeout_seconds)
                        if self.reader.is_alive():
                            errors.append(ExecutionUnknown("Attach reader teardown did not complete"))
                    except BaseException as error:
                        errors.append(error)
                try:
                    self.wire.close()
                except BaseException as error:
                    errors.append(error)
        finally:
            self.closed = True
        if errors:
            self.errors.extend("teardown:" + type(error).__name__ + ":" + str(error)[:256] for error in errors)
            raise errors[0]


class WorkerExecution:
    """Fresh one-shot owner; no history resubmission or uncertain redispatch."""
    def __init__(self, root: Path, store: GitStore, registration: WorkerRegistration, profile: WorkerProfile,
                 policy: WorkerPolicy, *, observation_admission: admission.ObservationAdmission,
                 checkpoint_authority: head.ExternalHead, delta_root: Path, cleanup_root: Path,
                 endpoint: engine.EngineEndpoint):
        require(type(registration) is WorkerRegistration and type(profile) is WorkerProfile
                and type(policy) is WorkerPolicy and type(observation_admission) is admission.ObservationAdmission
                and type(checkpoint_authority) is head.ExternalHead and type(endpoint) is engine.EngineEndpoint,
                "Exact physical worker authority types required")
        self.root, self.delta_root, self.cleanup_root = (Path(x).absolute() for x in (root, delta_root, cleanup_root))
        roots = (self.root, self.delta_root, self.cleanup_root, checkpoint_authority.root)
        require(all(x.resolve() == x and not x.is_symlink() for x in roots)
                and all(not a.is_relative_to(b) and not b.is_relative_to(a)
                        for index, a in enumerate(roots) for b in roots[index + 1:])
                and checkpoint_authority.journal_roots == (self.root, self.delta_root),
                "Disjoint canonical external owner roots required")
        require(not any(x.exists() for x in (self.root, self.delta_root, self.cleanup_root)),
                "Never overwrite/reopen a worker history; preserve failed originals")
        self.store, self.registration, self.profile, self.policy = store, registration, profile, policy
        self.endpoint, self.checkpoint_authority = endpoint, checkpoint_authority
        self.admission = observation_admission
        self.mode = "physical"
        self.closed = False
        self._cleanup_mode = False
        self._history_deadline = time.monotonic() + policy.lifetime_seconds
        self._work_deadline = self._history_deadline - CLEANUP_SECONDS
        self.emergency_cleanup: emergency.CleanupChannel | None = None
        self._cleanup_claims: dict[str, str] = {}
        self._live: dict[str, _LiveProcess] = {}
        self.tree, self.files = capture_git_source(store, registration.commit_oid)
        self.inputs = profile.case.inputs()
        self.sources = evaluator_sources()
        admission.verify_loaded_sources(self.sources)
        self.runtime = engine.runtime_identity(endpoint, policy.image_id, timeout_seconds=policy.transport_timeout_seconds)
        self.binding = binding_for(self.files, profile, policy, self.runtime)
        require(self.tree == registration.tree_oid and self.binding == registration.binding
                and self.sources == _LOADED_SOURCES, "Source/runtime/loaded evaluator binding differs")
        declared = registration.observation
        self.actual_registration = observation_registration_for(self.binding, profile, policy,
            subject=declared.gate.binding.subject, gate_id=declared.gate.gate_id, commit_oid=registration.commit_oid,
            tree_oid=self.tree, repetition_id=registration.repetition_id, cohort_trajectory_ids=declared.cohort_trajectory_ids)
        require(self.actual_registration == declared, "Declared worker admission differs")
        self.retained_freeze = self.admission.before_intent(self.actual_registration)
        self.config = {"protocol": PROTOCOL, "registration": asdict(registration), "profile": profile.record(),
            "policy": asdict(policy), "sources": self.sources, "runtime": self.runtime, "endpoint": asdict(endpoint),
            "source_manifest": source_manifest(self.files), "input_manifest": source_manifest(self.inputs),
            "hook_interface": hooks.interface(), "owner_roots": [str(x) for x in roots],
            "mode": "physical", "scope_credit": False, "global_independent_acceptance": False}
        self.journal = owner_journal.OwnerJournal(self.root, self.delta_root,
            context={"protocol": PROTOCOL, "config_sha256": digest(self.config),
                     "registration_sha256": digest(asdict(registration)), "purpose": self.binding.purpose},
            authority=checkpoint_authority)
        try:
            self._retain("config.json", encoded(self.config))
        except BaseException:
            self.journal.close()
            raise

    def checkpoint(self) -> chain.PrefixCommitment:
        require(not self.closed, "Closed worker owner")
        return self.journal.checkpoint()

    def read_authenticated(self, name: str) -> bytes:
        require(not self.closed, "Closed worker owner")
        return self.journal.read(name)

    def _retain(self, name: str, raw: bytes) -> None:
        require(re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", name) is not None
                and type(raw) is bytes and len(raw) <= MAX_RECORD_BYTES, "Bounded immutable evidence required")
        self.journal.retain(name, raw, cleanup=self._cleanup_mode)
        require(self.journal.read(name) == raw, "Acknowledged original changed")
        if name.endswith("-request.bin") or name.endswith("-command-intent.json"):
            self.checkpoint()

    def _remaining(self, requested: float) -> float:
        bound = self._history_deadline if self._cleanup_mode else self._work_deadline
        remaining = min(requested, bound - time.monotonic())
        require(remaining > 0, "Declared worker history deadline reached")
        return remaining

    def _deadline_after(self, seconds: float) -> float:
        return time.monotonic() + self._remaining(seconds)

    def _unchanged(self) -> None:
        self.checkpoint()
        self.admission.check_current(self.actual_registration, self.retained_freeze)
        require(self.sources == evaluator_sources() == _LOADED_SOURCES, "Loaded evaluator changed")
        tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files, "Registered Git source changed")
        runtime = engine.runtime_identity(self.endpoint, self.policy.image_id,
            timeout_seconds=self._remaining(self.policy.transport_timeout_seconds))
        require(runtime == self.runtime and binding_for(files, self.profile, self.policy, runtime) == self.binding,
                "Runtime/whole history/input binding changed")
        if hasattr(self, "workspace"):
            finite._verify_tree(self.workspace, self.files)
            finite._verify_tree(self.inputs_root, self.inputs)

    def _command(self, label: str, argv: list[str], timeout: float | None = None) -> dict[str, Any]:
        """Bound control traffic; candidate streams never pass through this helper."""
        self.checkpoint()
        require(self.mode == "physical" and self.endpoint is not None, "Control commands require a physical endpoint")
        assert self.endpoint is not None
        self.endpoint.validate()
        require(bool(argv) and argv[0] == "docker", "Only fixed Docker control commands allowed")
        argv = ["docker", "--host", "unix://" + self.endpoint.socket_path, *argv[1:]]
        environment = {key: value for key, value in DockerValidator._environment().items()
                       if key not in ("DOCKER_HOST", "DOCKER_CONTEXT")}
        timeout = self._remaining(self.policy.transport_timeout_seconds if timeout is None else timeout)
        command_deadline = self._deadline_after(timeout)
        self._retain(label + "-command-intent.json", encoded({"argv": argv,
            "runtime_sha256": digest(self.runtime), "timeout_seconds": timeout}))
        self.checkpoint()
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 stdin=subprocess.DEVNULL, env=environment)
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        counts = {"stdout": 0, "stderr": 0}
        errors: list[str] = []
        lock = threading.Lock()
        def drain(kind: str, pipe: Any) -> None:
            try:
                while chunk := pipe.read(65536):
                    with lock:
                        counts[kind] += len(chunk)
                        remaining = CONTROL_LIMIT - len(buffers[kind])
                        if remaining > 0:
                            buffers[kind].extend(chunk[:remaining])
                        if counts[kind] > CONTROL_LIMIT:
                            child.kill()
            except OSError:
                errors.append(kind)
            finally:
                pipe.close()
        workers: list[threading.Thread] = []
        timed_out = False
        setup_failure: BaseException | None = None
        teardown_deadline = min(self._history_deadline, command_deadline + self.policy.transport_timeout_seconds)
        try:
            for kind in buffers:
                worker = threading.Thread(target=drain, args=(kind, getattr(child, kind)), daemon=True)
                workers.append(worker)
                worker.start()
            child.wait(timeout=max(0, command_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True
        except BaseException as error:
            setup_failure = error
            errors.append("command-setup:" + type(error).__name__ + ":" + str(error)[:256])
        finally:
            if child.poll() is None:
                try:
                    child.kill()
                except OSError as error:
                    errors.append("kill:" + str(error)[:256])
            try:
                child.wait(timeout=max(0, teardown_deadline - time.monotonic()))
            except (OSError, subprocess.TimeoutExpired) as error:
                errors.append("reap-unproven:" + type(error).__name__)
            for worker in workers:
                if worker.ident is not None:
                    try:
                        worker.join(timeout=max(0, teardown_deadline - time.monotonic()))
                    except BaseException as error:
                        errors.append("drain-join:" + type(error).__name__)
            # A pipe whose drain never started is ours to close directly. Do
            # not block closing a pipe still held by a live drain thread.
            for index, kind in enumerate(buffers):
                if index >= len(workers) or workers[index].ident is None or not workers[index].is_alive():
                    try:
                        getattr(child, kind).close()
                    except OSError as error:
                        errors.append("pipe-close:" + str(error)[:256])
            if any(worker.is_alive() for worker in workers):
                errors.append("drain-teardown-unproven")
        with lock:
            data = {kind: bytes(raw) for kind, raw in buffers.items()}
            observed = dict(counts)
        record: dict[str, Any] = {"argv": argv, "exit_code": child.returncode, "timed_out": timed_out,
            "capture_complete": not errors and all(not worker.is_alive() for worker in workers),
            "observation_deadline_monotonic": command_deadline, "control_errors": errors}
        for kind, raw in data.items():
            name = label + "-" + kind + ".bin"
            self._retain(name, raw)
            record[kind] = {"path": name, "sha256": sha256(raw), "bytes": len(raw),
                            "observed_bytes": observed[kind], "truncated": observed[kind] != len(raw)}
        self._retain(label + ".json", encoded(record))
        self.checkpoint()
        if setup_failure is not None:
            raise setup_failure
        return record

    def _raw(self, record: dict[str, Any], kind: str = "stdout") -> bytes:
        item = record[kind]
        require(type(item) is dict and type(item.get("path")) is str
                and re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", item["path"]) is not None, "Invalid raw artifact reference")
        raw = self.read_authenticated(item["path"])
        require(sha256(raw) == item["sha256"] and len(raw) == item["bytes"], "Raw artifact differs")
        return raw

    @staticmethod
    def _clean(record: dict[str, Any]) -> bool:
        return (record["exit_code"] == 0 and not record["timed_out"] and record["capture_complete"]
                and not record["stdout"]["truncated"] and not record["stderr"]["truncated"])

    def _checked(self, label: str, argv: list[str]) -> dict[str, Any]:
        record = self._command(label, argv)
        require(self._clean(record), "Docker control command incomplete: " + label)
        return record

    def _inspect(self, label: str, container_id: str) -> dict[str, Any]:
        self.checkpoint()
        require(self.endpoint is not None and _SHA.fullmatch(container_id) is not None, "Full owned ID required")
        assert self.endpoint is not None
        result = engine._json_control(self.endpoint, "/containers/" + container_id + "/json",
            deadline=self._deadline_after(self.policy.transport_timeout_seconds), retain=self._retain, label=label)
        self.checkpoint()
        return result

    def _control(self, label: str, container_id: str, operation: str) -> tuple[int, bytes]:
        self.checkpoint()
        require(self.endpoint is not None and _SHA.fullmatch(container_id) is not None
                and operation in ("start", "stop?t=" + str(self.policy.stop_timeout_seconds)), "Closed lifecycle action required")
        assert self.endpoint is not None
        result = engine._control(self.endpoint, "POST", "/containers/" + container_id + "/" + operation,
            deadline=self._deadline_after(self.policy.transport_timeout_seconds + self.policy.stop_timeout_seconds),
            retain=self._retain, label=label)
        self.checkpoint()
        return result

    def _compare(self, label: str, before: dict[str, Any], after: dict[str, Any], spec: RoleSpec, phase: str) -> dict[str, Any]:
        comparison = role_identity_comparison(before, after, spec, self.runtime, phase)
        self._retain(label + ".json", encoded(comparison))
        require(comparison["matches"] is True, "Role identity/epoch continuity differs")
        return comparison

    def _boundary(self, label: str, baseline: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        value = self._inspect(label, baseline["Id"])
        validate_state(value, "running")
        return self._compare(label + "-continuity", baseline, value, spec, "running-to-running")

    def _create(self, label: str, spec: RoleSpec, owned: dict[str, tuple[RoleSpec, str | None]]) -> dict[str, Any]:
        absent = self._checked(label + "-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        require(not self._raw(absent).strip(), "Container name already exists")
        argv = create_argv(spec, self.policy.image_id)
        self._retain(label + "-create-intent.json", encoded({"spec": asdict(spec), "create_argv": argv}))
        require(self.emergency_cleanup is not None, "Independent cleanup ownership channel required")
        assert self.emergency_cleanup is not None
        claim = self.emergency_cleanup.claim_container(name=spec.name, labels=dict(spec.labels), argv=spec.argv,
            preabsence_record=label + "-before.json")
        self._cleanup_claims[spec.name] = claim
        owned[spec.name] = (spec, None)  # Preabsence + durable intent cover an uncertain create response.
        record = self._checked(label + "-create", argv)
        container_id = self._raw(record).strip().decode("ascii")
        require(_SHA.fullmatch(container_id) is not None, "Invalid created container ID")
        self.emergency_cleanup.confirm_container(claim, create_record=label + "-create.json")
        owned[spec.name] = (spec, container_id)
        value = self._inspect(label + "-created", container_id)
        validate_role(value, spec, self.policy.image_id, self.runtime)
        validate_state(value, "created")
        require(value["Id"] == container_id, "Created inspect names a different container")
        return value

    def _start_role(self, label: str, created: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        require(spec.role in ("server", "keeper"), "Long-lived start requires server or keeper role")
        before = self._inspect(label + "-prestart", created["Id"])
        validate_state(before, "created")
        self._compare(label + "-prestart-comparison", created, before, spec, "created-to-prestart")
        self._retain(label + "-start-intent.json", encoded({"container_id": created["Id"], "spec": asdict(spec),
            "prestart_sha256": digest(before), "controller_action": "start-long-lived-role"}))
        code, body = self._control(label + "-start", created["Id"], "start")
        self._retain(label + "-start-completion.json", encoded({"status": code, "body_sha256": sha256(body),
            "framing_complete": True, "eof_observed": True, "container_id": created["Id"]}))
        require(code == 204, "Long-lived role start rejected")
        running = self._inspect(label + "-running", created["Id"])
        validate_state(running, "running")
        self._compare(label + "-startup-comparison", created, running, spec, "created-to-running")
        self._retain(label + "-running.json", encoded(running))
        return running

    def _remove(self, label: str, spec: RoleSpec, container_id: str | None, *, force: bool) -> bool:
        """Remove only independently matched resources from the durable create intent."""
        inspected = self._command(label + "-inspect", ["docker", "inspect", "--format", "{{json .}}", spec.name])
        if not self._clean(inspected):
            absent = self._command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
            return self._clean(absent) and not self._raw(absent).strip()
        value = engine.strict_json_loads(self._raw(inspected))
        require(type(value) is dict and _SHA.fullmatch(value.get("Id", "")) is not None
                and (container_id is None or value["Id"] == container_id) and value.get("Name") == "/" + spec.name
                and value.get("Image") == self.policy.image_id and value.get("Config", {}).get("Labels") == dict(spec.labels),
                "Cleanup ownership differs; refusing removal")
        self._retain(label + "-intent.json", encoded({"container_id": value["Id"], "spec": asdict(spec),
            "force": force, "inspection_sha256": digest(value)}))
        require(self.emergency_cleanup is not None and spec.name in self._cleanup_claims,
                "Original acknowledged cleanup claim required")
        assert self.emergency_cleanup is not None
        self.emergency_cleanup.note_normal_removal(self._cleanup_claims[spec.name])
        removed = self._command(label + "-remove", ["docker", "rm", *(["--force"] if force else []), value["Id"]])
        absent = self._command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        complete = self._clean(removed) and self._clean(absent) and not self._raw(absent).strip()
        if complete:
            self.emergency_cleanup.confirm_normal_removal(self._cleanup_claims[spec.name],
                remove_record=label + "-remove.json", absence_record=label + "-absence.json")
        return complete

    def _labels(self, role: str, step_id: str) -> tuple[tuple[str, str], ...]:
        return tuple(sorted({"gossip.execution": self.execution_id, "gossip.role": role,
            "gossip.source": self.binding.source_sha256, "gossip.fixture": self.binding.fixture_sha256,
            "gossip.helper": self.binding.helper_sha256, "gossip.step": step_id}.items()))

    def _keeper_boundary(self, label: str) -> None:
        self._boundary(label + "-keeper", self.keeper, self.keeper_spec)
        inspected = self._checked(label + "-volume", ["docker", "volume", "inspect", "--format", "{{json .}}", self.volume])
        require(engine.strict_json_loads(self._raw(inspected)) == self.volume_baseline,
                "Held database volume identity changed")

    def _trusted(self, label: str, source: str, args: tuple[str, ...] = (), *, worker: str | None = None,
                 timeout: float | None = None) -> dict[str, Any]:
        self._keeper_boundary(label + "-before")
        if worker is None:
            cid = self.keeper["Id"]
        else:
            live = self._live[worker]
            require(live.running is not None, "Trusted kernel read requires a current live worker")
            assert live.running is not None
            self._boundary(label + "-worker-before", live.running, live.spec)
            cid = live.created["Id"]
        argv = ["docker", "exec", "--user=65534:65534", cid, "python", "-I", "-c", source, *args]
        self._retain(label + "-trusted-input.json", encoded({"source_sha256": sha256(source.encode()), "argv": argv,
            "candidate_imports": False, "container_id": cid, "worker_pid_namespace": worker is not None}))
        record = self._command(label, argv, timeout=timeout)
        require(self._clean(record), "Trusted observation process failed or exceeded its bound")
        require(self._raw(record, "stderr") == b"", "Trusted observation stderr requires review")
        self._keeper_boundary(label + "-after")
        if worker is not None:
            live = self._live[worker]
            assert live.running is not None
            self._boundary(label + "-worker-after", live.running, live.spec)
        value = engine.strict_json_loads(self._raw(record))
        require(type(value) is dict, "Trusted observation object required")
        return {"record": record, "value": value}

    def _hook_observe(self, label: str, worker: str) -> dict[str, Any]:
        # Polling has a fixed deadline and reads no HTTP/application route. The
        # marker is retained as a candidate claim; no inferred phase on timeout.
        directory = hooks.ROOT + "/" + worker
        source = hooks.EVENT_SOURCE
        observed = self._trusted(label + "-event", source, (directory, str(self.policy.hook_timeout_seconds)),
            timeout=self.policy.hook_timeout_seconds + self.policy.transport_timeout_seconds)
        result: dict[str, Any] = {"event": observed, "kernel": None, "phase_authority": False}
        if observed["value"].get("state") == "observed":
            result["kernel"] = self._trusted(label + "-kernel", hooks.KERNEL_SOURCE, (directory,), worker=worker)
        return result

    def _hook_release(self, label: str, worker: str) -> dict[str, Any]:
        source = hooks.RELEASE_SOURCE
        result = self._trusted(label, source, (hooks.ROOT + "/" + worker,))
        require(result["value"] == {"released": True, "acknowledgement": "released"},
                "Hook release was not acknowledged before instrumentation expiry")
        self._live[worker].release_label = label
        return result

    def _instrumentation_guard(self, label: str, tracked: dict[str, _LiveProcess]) -> bool:
        now = time.monotonic_ns()
        bounds = []
        for worker, live in sorted(tracked.items()):
            require(type(live.start_before_ns) is int, "Original host start bound required")
            assert live.start_before_ns is not None
            # A release first acknowledged during this action must still fit
            # the start-derived bound. Later actions join that prior proof.
            released = live.release_label if live.release_label is not None and not live.release_label.startswith(label[:4] + "-") else None
            valid = (released is not None or 0 <= now - live.start_before_ns < hooks.MAX_HOLD_SECONDS * 1_000_000_000)
            bounds.append({"worker": worker, "container_id": live.created["Id"],
                "start_label": live.label, "start_before_ns": live.start_before_ns,
                "maximum_hold_seconds": hooks.MAX_HOLD_SECONDS, "release_acknowledgement_label": released,
                "within_bound": valid})
        record = {"protocol": "worker-hook-host-bound-v1", "clock": "host-monotonic-ns",
                  "checked_ns": now, "bounds": bounds}
        self._retain(label + "-instrumentation.json", encoded(record))
        return all(bound["within_bound"] for bound in bounds)

    def _cli(self, label: str, args: list[str], row: dict[str, Any]) -> dict[str, Any]:
        argv = (*cases.CLI_PREFIX, *args)
        spec = RoleSpec("cli", "gossip-" + self.execution_id + "-" + label, argv,
            self._labels("cli", label), (("/workspace", str(self.workspace)), ("/inputs", str(self.inputs_root))), self.volume)
        self._keeper_boundary(label + "-before")
        created = self._create(label, spec, self.owned)
        self._unchanged()
        process = retained_process.run_process(self.endpoint, container_id=created["Id"], expected=created,
            policy=self.policy.process, retain=self._retain, label=label + "-process", expected_runtime=self.runtime,
            expected_argv=list(argv))
        self._retain(label + "-process-result.json", encoded(process))
        row.update(process=process, cli_id=created["Id"], removed=False)
        self._keeper_boundary(label + "-after")
        self._unchanged()
        # Preserve the complete finite result even if cleanup/continuity later
        # fails. The observer decides availability from its actual capture.
        removed = self._remove(label + "-retire", spec, created["Id"], force=True)
        self.cleanup[spec.name] = removed
        require(removed, "Finite CLI cleanup unproven")
        return {"process": process, "cli_id": created["Id"], "removed": removed}

    def _worker_start(self, label: str, action: dict[str, Any]) -> dict[str, Any]:
        worker = action["worker"]
        require(worker not in self._live and not any(not process.closed for process in self._live.values()),
                "Only one long-lived worker per history; competitor is a distinct finite CLI")
        source = hooks.SETUP_HOOK_SOURCE
        self._trusted(label + "-hook-setup", source, (worker,))
        argv = ("python", "-B", "/inputs/__evaluator__/worker_launcher_v1.py", "/inputs/__evaluator__/" + worker + ".json",
                *cases.CLI_PREFIX[3:], *action["args"])
        spec = RoleSpec("cli", "gossip-" + self.execution_id + "-" + worker, argv,
            self._labels("worker", label), (("/workspace", str(self.workspace)), ("/inputs", str(self.inputs_root))), self.volume)
        created = self._create(label, spec, self.owned)
        live = _LiveProcess(self, label + "-worker", spec, created)
        live.hook_mode = action["mode"]
        self._live[worker] = live  # Before start: cleanup knows even an uncertain start.
        self._unchanged()
        running = live.start()
        self._keeper_boundary(label + "-after")
        return {"worker": worker, "container_id": created["Id"], "start_label": label + "-worker",
                "running": running, "hook_phase": action["phase"], "hook_mode": action["mode"]}

    def _worker_finish(self, label: str, worker: str, row: dict[str, Any], *, kill: bool) -> dict[str, Any]:
        live = self._live[worker]
        require(not live.closed, "Worker already retired")
        result = live.finish(kill=kill, row=row)
        row.update(result)
        # Keep one held keeper + exact volume throughout every worker epoch.
        self._keeper_boundary(label + "-after")
        removed = self._remove(label + "-retire", live.spec, live.created["Id"], force=False)
        self.cleanup[live.spec.name] = removed
        require(removed, "Prior worker still present before subsequent epoch")
        result["removed"] = removed
        return result

    def execute_once(self) -> chain.PrefixCommitment:
        self._unchanged()
        require(not self.journal.has("intent.json"), "Never redispatch a retained worker intent")
        self.execution_id = "worker-" + uuid.uuid4().hex
        self.volume = "gossip-" + self.execution_id + "-volume"
        self.owned: dict[str, tuple[RoleSpec, str | None]] = {}
        self.cleanup: dict[str, bool] = {}
        self._retain("intent.json", encoded({"protocol": PROTOCOL, "execution_id": self.execution_id,
            "binding_sha256": digest(asdict(self.binding)), "profile_sha256": self.profile.sha256,
            "ordered_actions": self.profile.case.record["input"]["actions"], "volume": self.volume,
            "no_automatic_retry": True, "physical": True}))
        rows: list[dict[str, Any]] = []
        infrastructure: list[str] = []
        failure: BaseException | None = None
        volume_clean = False
        with tempfile.TemporaryDirectory(prefix="gossip-worker-stage-") as temporary:
            staging = Path(temporary).resolve()
            staging.chmod(0o755)
            self.workspace, self.inputs_root = staging / "workspace", staging / "inputs"
            for root, files in ((self.workspace, self.files), (self.inputs_root, self.inputs)):
                root.mkdir(mode=0o755)
                for name, raw in files.items():
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    path.write_bytes(raw)
                    path.chmod(0o444)
                finite._verify_tree(root, files)
            self.emergency_cleanup = emergency.CleanupChannel.create(self.cleanup_root, journal=self.journal,
                endpoint=self.endpoint, runtime=self.runtime, source_sha256=self.binding.source_sha256,
                fixture_sha256=self.binding.fixture_sha256, execution_id=self.execution_id, image_id=self.policy.image_id,
                candidate_mount_roots=(staging,), journal_roots=(self.root, self.delta_root, self.checkpoint_authority.root))
            self._retain("staging.json", encoded({"workspace": str(self.workspace), "inputs": str(self.inputs_root),
                "source_manifest": source_manifest(self.files), "input_manifest": source_manifest(self.inputs)}))
            try:
                self._setup()
                for action in self.profile.case.record["input"]["actions"]:
                    self._unchanged()
                    label, operation = action["id"], action["op"]
                    self._retain(label + "-step-intent.json", encoded(action))
                    row: dict[str, Any] = {"step_id": label, "op": operation, "state": "unavailable"}
                    rows.append(row)
                    tracked = {worker: live for worker, live in self._live.items()
                               if live.started and not live.closed and live.hook_mode == "pause"}
                    step_failure: BaseException | None = None
                    try:
                        require(self._instrumentation_guard(label + "-before", tracked),
                                "Evaluator hold bound expired before action")
                        if operation == "cli":
                            row.update(self._cli(label, action["args"], row))
                        elif operation == "worker_start":
                            row.update(self._worker_start(label, action))
                        elif operation == "snapshot":
                            row.update(self._trusted(label + "-snapshot", hooks.SNAPSHOT_SOURCE))
                        elif operation == "hook_observe":
                            row.update(self._hook_observe(label, action["worker"]))
                        elif operation == "hook_release":
                            row.update(self._hook_release(label + "-release", action["worker"]))
                        elif operation in ("worker_kill", "worker_wait"):
                            row.update(self._worker_finish(label, action["worker"], row, kill=operation == "worker_kill"))
                        else:
                            raise ExecutionError("Unknown frozen action")
                    except BaseException as error:
                        step_failure = error
                    # Retain timing even on a partial result so a complete
                    # wrong exit can survive attach loss within a valid hold.
                    tracked.update({worker: live for worker, live in self._live.items()
                                    if live.started and not live.closed and live.hook_mode == "pause"})
                    try:
                        require(self._instrumentation_guard(label + "-after", tracked),
                                "Evaluator hold bound expired during action")
                    except BaseException as error:
                        if step_failure is None:
                            step_failure = error
                    if step_failure is not None:
                        raise step_failure
                    row["state"] = "observed"
                    self._retain(label + "-observation.json", encoded(row))
                    # Missing hook cannot be timed into an invented phase. The
                    # history stops and emergency cleanup handles the live role.
                    if operation == "hook_observe":
                        require(row["event"]["value"].get("state") == "observed", "Declared hook was not observed")
                self._unchanged()
            except BaseException as error:
                failure = error
                infrastructure.append(type(error).__name__ + ":" + str(error)[:512])
                if rows and not self.journal.uncertain:
                    partial_name = rows[-1]["step_id"] + "-observation.json"
                    try:
                        if not self.journal.has(partial_name):
                            self._retain(partial_name, encoded(rows[-1]))
                    except BaseException:
                        # Do not replace the original operation failure or heal
                        # a failed retention. Cleanup uses only prior ownership.
                        pass
            finally:
                # Close attach threads before emergency cleanup, including on a
                # retention failure. Close never fabricates an EOF/completion.
                for live in self._live.values():
                    if not live.closed:
                        try:
                            live.close()
                        except BaseException as error:
                            infrastructure.append("attach-teardown:" + type(error).__name__ + ":" + str(error)[:256])
                            if failure is None:
                                failure = error
                        if not self.journal.uncertain:
                            try:
                                if live.wire is not None:
                                    self._retain(live.label + "-aborted-attach.bin", bytes(live.wire.raw))
                                self._retain(live.label + "-aborted.json", encoded({"complete": False,
                                    "errors": live.errors, "original_failure": infrastructure,
                                    "eof_after_start": live.eof_after_start}))
                            except BaseException as error:
                                if failure is None:
                                    failure = error
                self._cleanup_mode = True
                if self.emergency_cleanup is not None:
                    try:
                        result = self.emergency_cleanup.run(reason="worker history complete" if failure is None else
                            type(failure).__name__ + ":" + str(failure)[:256])
                        # Emergency diagnostics never heal an uncertain main chain.
                        if not self.journal.uncertain:
                            self._retain("cleanup-result.json", encoded(asdict(result)))
                        volume_clean = result.all_resources_absent
                        if not volume_clean:
                            infrastructure.extend(result.uncertainties)
                    except BaseException as error:
                        infrastructure.append("emergency-cleanup:" + type(error).__name__ + ":" + str(error)[:256])
                        if failure is None:
                            failure = error
            if not self.journal.uncertain:
                self._retain("terminal.json", encoded({"protocol": PROTOCOL, "execution_id": self.execution_id,
                    "steps": rows, "planned_actions": len(self.profile.case.record["input"]["actions"]),
                    "observed_action_records": sum(row["state"] == "observed" for row in rows),
                    "cleanup_verified": volume_clean, "normal_removals": self.cleanup,
                    "infrastructure": infrastructure, "purpose": self.binding.purpose,
                    "product_acceptance": False, "independent_purpose_credit": False}))
                final = self.checkpoint()
            else:
                raise ExecutionUnknown("Uncertain worker journal; only separately retained cleanup/prior facts remain") from failure
        if failure is not None and not isinstance(failure, (OSError, ValueError, subprocess.SubprocessError)):
            raise failure
        return final

    def _setup(self) -> None:
        assert self.emergency_cleanup is not None
        before = self._checked("volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + self.volume + "$"])
        require(not self._raw(before).strip(), "Volume name already exists")
        labels = {"gossip.execution": self.execution_id, "gossip.snapshot": SNAPSHOT_PROTOCOL}
        self._cleanup_claims[self.volume] = self.emergency_cleanup.claim_volume(name=self.volume,
            labels=labels, options=VOLUME_OPTIONS, preabsence_record="volume-before.json")
        argv = ["docker", "volume", "create", "--driver", "local"]
        for key, value in labels.items():
            argv += ["--label", key + "=" + value]
        for key, value in VOLUME_OPTIONS.items():
            argv += ["--opt", key + "=" + value]
        created = self._checked("volume-create", [*argv, self.volume])
        require(self._raw(created).strip() == self.volume.encode(), "Created volume identity differs")
        inspected = self._checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", self.volume])
        self.volume_baseline = engine.strict_json_loads(self._raw(inspected))
        require(self.volume_baseline.get("Name") == self.volume and self.volume_baseline.get("Driver") == "local"
                and self.volume_baseline.get("Options") == VOLUME_OPTIONS and self.volume_baseline.get("Labels") == labels,
                "Owned volume policy differs")
        self._retain("volume-baseline.json", encoded(self.volume_baseline))
        self.emergency_cleanup.confirm_volume(self._cleanup_claims[self.volume], baseline_record="volume-baseline.json")
        self.keeper_spec = RoleSpec("keeper", "gossip-" + self.execution_id + "-keeper",
            ("python", "-I", "-c", "import time;time.sleep(" + str(self.policy.lifetime_seconds) + ")"),
            self._labels("keeper", "state-lifetime"), (), self.volume)
        keeper_created = self._create("keeper", self.keeper_spec, self.owned)
        self.keeper = self._start_role("keeper", keeper_created, self.keeper_spec)
        self._trusted("initial-directories", hooks.INITIAL_DIRECTORY_SOURCE)

    def close(self) -> None:
        if self.closed:
            return
        errors: list[BaseException] = []
        closers = [live.close for live in self._live.values()]
        if self.emergency_cleanup is not None:
            closers.append(self.emergency_cleanup.close)
        closers.append(self.journal.close)
        try:
            for close in closers:
                try:
                    close()
                except BaseException as error:
                    errors.append(error)
        finally:
            self.closed = True
        if errors:
            raise errors[0]

    def __enter__(self) -> WorkerExecution:
        return self

    def __exit__(self, exc_type: Any, original: Any, traceback: Any) -> None:
        try:
            self.close()
        except BaseException as error:
            if original is None:
                raise
            if hasattr(original, "add_note"):
                original.add_note("Worker resource teardown also failed: " + type(error).__name__ + ":" + str(error)[:256])


_LOADED_SOURCES = evaluator_sources()
