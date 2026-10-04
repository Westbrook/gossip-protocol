"""Owned source-bound browser/process observations; no supplied-result entry point.

Only prospective public-release admission is wired here. The existing frozen
browser and product-process evaluators are unchanged. Every route is retained
before a distinct trusted helper dispatch into the current server namespace.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass
import base64
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import tempfile
import threading
import time
from typing import Any
import uuid

from . import candidate_product_process_execution_v1 as base
from . import candidate_product_browser_cases_v1 as cases
from . import candidate_product_browser_transport_v1 as transport
from . import candidate_http_transport_v1 as wire
from . import candidate_client_execution_v4 as finite
from . import candidate_client_process_v4 as engine
from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_checkpoint_head_v1 as head
from . import candidate_execution_journal_v1 as owner_journal
from . import candidate_emergency_cleanup_v1 as emergency
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry
from .candidate_release_execution_v2 import capture_git_source
from .candidate_observation_admission_v1 import source_manifest, source_sha256
from .gitstore import GitStore
from .sandbox import DockerValidator

PROTOCOL = "candidate-product-browser-execution-v1"
IPC_PROTOCOL = "candidate-product-browser-ipc-v1"
SNAPSHOT_PROTOCOL = "docker-owned-browser-held-tmpfs-v1"
CONTROL_LIMIT = 8 * 1024 * 1024
MAX_RECORD_BYTES = 32 * 1024 * 1024
CLEANUP_SECONDS = 300
VOLUME_OPTIONS = dict(base.VOLUME_OPTIONS)
RoleSpec = base.RoleSpec
create_argv, validate_role, validate_state = base.create_argv, base.validate_role, base.validate_state
role_identity_comparison = transport.role_identity_comparison
require, ExecutionError, ExecutionUnknown = base.require, base.ExecutionError, base.ExecutionUnknown
encoded, sha256 = cases.encoded, cases.sha
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_NAME = re.compile(r"[a-z][a-z0-9.-]{0,100}\Z")
DRIVER_ROOT = Path(__file__).resolve().parents[1] / "devtools/browser"
BASE_PROJECT = Path(base.__file__).resolve().parents[1]
DRIVER = DRIVER_ROOT / "candidate_product_browser_driver_v1.cjs"
PROTOCOL_SOURCE = DRIVER_ROOT / "candidate_product_browser_protocol_v1.cjs"


def digest(value: Any) -> str:
    return sha256(encoded(value))


def evaluator_sources() -> dict[str, str]:
    result = dict(base.evaluator_sources())
    for name in ("cases", "execution", "observation", "fixture", "transport"):
        path = Path(__file__).with_name("candidate_product_browser_" + name + "_v1.py")
        result["gossip_harness/" + path.name] = sha256(path.read_bytes())
    for path in (DRIVER, PROTOCOL_SOURCE):
        result["devtools/browser/" + path.name] = sha256(path.read_bytes())
    for name in ("lifecycle.cjs", "package.json", "package-lock.json"):
        result["devtools/browser/" + name] = sha256((BASE_PROJECT / "devtools/browser" / name).read_bytes())
    return result


@dataclass(frozen=True)
class BrowserPolicy:
    image_id: str
    node_executable: str
    node_modules_path: str
    lifetime_seconds: int = 1200
    transport_timeout_seconds: int = 15
    stop_timeout_seconds: int = 5
    maximum_requests: int = 256
    message_limit: int = 8 * 1024 * 1024
    action_seconds: int = 15
    seed: int = 0

    def __post_init__(self) -> None:
        for value in (self.node_executable, self.node_modules_path):
            require(type(value) is str and Path(value).is_absolute() and Path(value).resolve() == Path(value),
                    "Exact canonical installed Node/package paths required")
        require(self.lifetime_seconds == 1200 and self.transport_timeout_seconds == 15
                and self.stop_timeout_seconds == 5 and self.maximum_requests == 256
                and self.message_limit == 8 * 1024 * 1024 and self.action_seconds == 15 and self.seed == 0,
                "Closed prospective browser resource policy required")
        self.probe.__post_init__()

    @property
    def probe(self) -> base.HttpPolicy:
        return base.HttpPolicy(self.image_id, lifetime_seconds=self.lifetime_seconds)


def runtime_identity(policy: BrowserPolicy) -> dict[str, Any]:
    """Pinned runtime inventory only; this command never launches a browser."""
    policy.__post_init__()
    value = subprocess.run([policy.node_executable, str(PROTOCOL_SOURCE), "--probe"],
        env={"PATH": os.environ.get("PATH", ""), "NODE_PATH": policy.node_modules_path},
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
    require(value.returncode == 0 and not value.stderr and len(value.stdout) <= 65536,
            "Pinned Node/Playwright/browser inventory unavailable")
    result = engine.strict_json_loads(value.stdout)
    require(type(result) is dict and result["protocol"] == IPC_PROTOCOL
            and result["node_executable"] == policy.node_executable
            and result["protocol_sha256"] == sha256(PROTOCOL_SOURCE.read_bytes()), "Runtime probe differs")
    return result


@dataclass(frozen=True)
class BrowserProfile:
    case: cases.BrowserCase

    def __post_init__(self) -> None:
        require(type(self.case) is cases.BrowserCase, "Exact closed browser case required")
        self.case.__post_init__()

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return (*self.case.facet_ids, self.case.identifier + ":mechanics")

    def record(self) -> dict[str, Any]:
        return {"protocol": "candidate-product-browser-profile-v1", "definition": self.case.record,
                "ordered_case_ids": self.ordered_case_ids, "original_definition_purpose": cases.ORIGINAL_PURPOSE,
                "whole_product_acceptance": False, "native_browser_network_credit": False,
                "global_independent_acceptance": False, "aggregation": "known-failure-preserved-with-unknown"}

    @property
    def sha256(self) -> str:
        return digest(self.record())


@dataclass(frozen=True)
class BrowserBinding:
    source_sha256: str
    requirements_sha256: str
    recipe_sha256: str
    fixture_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    engine_runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    profile_sha256: str
    helper_sha256: str
    role_policy_sha256: str
    purpose: str = "public_release"
    milestone: str = "M4"
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        require(all(type(value) is str and _SHA.fullmatch(value) for key, value in asdict(self).items()
                    if key.endswith("_sha256")), "Exact immutable binding hashes required")
        require(self.requirements_sha256 == cases.CONTRACT_SHA256 and self.protocol == PROTOCOL
                and self.purpose == "public_release" and self.milestone == "M4", "New public-only browser admission required")


def binding_for(files: dict[str, bytes], profile: BrowserProfile, policy: BrowserPolicy,
                runtime: dict[str, Any], browser_runtime: dict[str, Any]) -> BrowserBinding:
    profile.__post_init__()
    return BrowserBinding(source_sha256(files), cases.CONTRACT_SHA256, profile.case.sha256,
        digest(source_manifest(profile.case.inputs())), digest(evaluator_sources()),
        digest({"engine": runtime, "browser": browser_runtime}), digest(runtime),
        digest({"environment": DockerValidator._environment(), "origin": cases.ORIGIN,
                "browser": "sandbox-closed-proxy-dns-fresh-home-context-blocked-serviceworkers-websockets",
                "volume_options": VOLUME_OPTIONS, "bridge": IPC_PROTOCOL}),
        digest({"policy": asdict(policy), "probe": asdict(policy.probe), "journal": asdict(chain.Limits()),
                "trace_bytes": 67108864, "screenshot_bytes": 16777216, "dom_bytes": 262144}),
        digest({"seed": 0, "meaning": "fixed public input"}), profile.sha256, wire.helper_sha256(),
        digest(transport.role_policy_identity()))


def observation_registration_for(binding: BrowserBinding, profile: BrowserProfile, policy: BrowserPolicy, *,
        subject: registry.Subject, gate_id: str, commit_oid: str, tree_oid: str, repetition_id: str,
        cohort_trajectory_ids: tuple[str, ...]) -> admission.ObservationRegistration:
    require(type(binding) is BrowserBinding and type(profile) is BrowserProfile and type(subject) is registry.Subject
            and subject.source_sha256 == binding.source_sha256 and subject.requirements_sha256 == cases.CONTRACT_SHA256
            and subject.milestone == "M4" and binding.profile_sha256 == profile.sha256, "Exact subject/profile required")
    gate_binding = registry.Binding(subject, digest({"profile": profile.record(), "case": profile.case.sha256}),
        binding.evaluator_sha256, policy.image_id.removeprefix("sha256:"),
        digest({"environment": binding.environment_sha256, "runtime": binding.runtime_sha256}),
        binding.limits_sha256, binding.seed_sha256, PROTOCOL, binding.purpose)
    gate = registry.Gate(gate_id, tuple(profile.case.record["requirements"]), profile.ordered_case_ids, gate_binding)
    return admission.ObservationRegistration(gate, commit_oid, tree_oid, repetition_id, cohort_trajectory_ids,
        profile.case.sha256, profile.sha256, cases.ORIGINAL_PURPOSE, admission.binding_sha256(binding, gate=gate))


@dataclass(frozen=True)
class BrowserRegistration:
    binding: BrowserBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    observation: admission.ObservationRegistration

    def __post_init__(self) -> None:
        require(type(self.binding) is BrowserBinding and type(self.observation) is admission.ObservationRegistration
                and all(type(x) is str and re.fullmatch(r"[0-9a-f]{40}", x) for x in (self.commit_oid, self.tree_oid))
                and _NAME.fullmatch(self.repetition_id) is not None, "Typed exact source registration required")


def bridge_request(value: Any) -> dict[str, Any]:
    require(type(value) is dict and set(value) == {"url", "method", "target", "headers", "body_b64", "body_sha256"},
            "Literal closed browser request required")
    target, method = value["target"], value["method"]
    require(type(target) is str and len(target.encode()) <= 4096 and value["url"] == cases.ORIGIN + target
            and method in ("GET", "POST"), "Fixed bridge origin/method required")
    raw = base64.b64decode(value["body_b64"], validate=True)
    require(len(raw) <= 65536 and base64.b64encode(raw).decode() == value["body_b64"]
            and sha256(raw) == value["body_sha256"], "Original browser body bytes differ")
    pairs = value["headers"]
    require(type(pairs) is list and len(pairs) <= 128 and all(type(pair) is list and len(pair) == 2
            and all(type(x) is str for x in pair) for pair in pairs), "Bounded header pairs required")
    names = [pair[0].lower() for pair in pairs]
    require(len(names) == len(set(names)) and not set(names).intersection({"cookie", "authorization", "proxy-authorization",
        "host", "connection", "content-length", "transfer-encoding", "trailer", "upgrade", "expect"}),
        "Unsupported request header representation")
    if method == "GET":
        require(not raw, "GET body outside bridge profile")
    else:
        require(target.startswith("/api/") and re.match(r"application/json(?:\s*;|$)",
            dict(zip(names, (pair[1] for pair in pairs))).get("content-type", ""), re.I) is not None,
            "POST JSON route required")
    recipe = {"method": method, "target": target, "headers": [["Host", "127.0.0.1:8765"], ["Connection", "close"],
              *pairs, *([["Content-Length", str(len(raw))]] if method == "POST" else [])], "body_b64": value["body_b64"]}
    wire.request_bytes(recipe, cases.PORT)
    return recipe


def bridge_response(observation: wire.WireObservation) -> dict[str, Any]:
    response = observation.response
    require(observation.sent_complete and observation.exchange_complete and response.body_complete
            and response.framing_complete and type(response.status_code) is int, "Complete unambiguous raw HTTP response unavailable")
    assert response.status_code is not None
    require(not 300 <= response.status_code < 400 and 200 <= response.status_code <= 599, "Bridge redirect/status unsupported")
    require(not response.informational and not response.trailers, "Informational/trailer semantics outside bridge profile")
    connections = [value for key, value in response.headers if key.lower() == "connection"]
    require(all(token.strip().lower() in ("close", "keep-alive") for value in connections for token in value.split(",")),
            "Connection-nominated extension headers outside bridge profile")
    headers: list[list[str]] = []
    names: set[str] = set()
    for key, value in response.headers:
        lower = key.lower()
        require(all(char == "\t" or 32 <= ord(char) <= 126 for char in value), "Non-ASCII response header representation unsupported")
        require(lower not in ("set-cookie", "location", "content-disposition", "content-encoding"), "Bridge response semantics unsupported")
        if lower in ("content-length", "transfer-encoding", "connection", "trailer", "upgrade", "keep-alive", "te", "proxy-connection"):
            continue
        require(lower not in names, "Duplicate end-to-end header cannot be represented by Playwright fulfill")
        names.add(lower)
        headers.append([key, value])
    return {"available": True, "status": response.status_code, "headers": headers,
            "body_b64": base64.b64encode(response.body).decode(), "body_sha256": sha256(response.body)}


class BrowserExecution:
    """One physical history with durable external authority and owned resources."""
    keeper: dict[str, Any]
    keeper_spec: RoleSpec
    volume_baseline: dict[str, Any]

    def __init__(self, root: Path, store: GitStore, registration: BrowserRegistration, profile: BrowserProfile,
                 policy: BrowserPolicy, *, observation_admission: admission.ObservationAdmission,
                 checkpoint_authority: head.ExternalHead, delta_root: Path, cleanup_root: Path,
                 endpoint: engine.EngineEndpoint):
        require(type(registration) is BrowserRegistration and type(profile) is BrowserProfile
                and type(policy) is BrowserPolicy and type(observation_admission) is admission.ObservationAdmission
                and type(checkpoint_authority) is head.ExternalHead and type(endpoint) is engine.EngineEndpoint,
                "Exact physical browser authority types required")
        self.root, self.delta_root, self.cleanup_root = (Path(x).absolute() for x in (root, delta_root, cleanup_root))
        roots = (self.root, self.delta_root, self.cleanup_root, checkpoint_authority.root)
        require(all(x.resolve() == x and not x.is_symlink() for x in roots)
                and all(not a.is_relative_to(b) and not b.is_relative_to(a)
                        for index, a in enumerate(roots) for b in roots[index + 1:])
                and checkpoint_authority.journal_roots == (self.root, self.delta_root), "Disjoint canonical roots required")
        require(not any(x.exists() for x in (self.root, self.delta_root, self.cleanup_root)), "Never overwrite an original history")
        self.store, self.registration, self.profile, self.policy = store, registration, profile, policy
        self.endpoint, self.checkpoint_authority, self.admission = endpoint, checkpoint_authority, observation_admission
        self.mode, self.closed, self._cleanup_mode = "physical", False, False
        self._history_deadline = time.monotonic() + policy.lifetime_seconds
        self._work_deadline = self._history_deadline - CLEANUP_SECONDS
        self.emergency_cleanup: emergency.CleanupChannel | None = None
        self._cleanup_claims: dict[str, str] = {}
        self.tree, self.files = capture_git_source(store, registration.commit_oid)
        self.inputs = profile.case.inputs()
        self.sources = evaluator_sources()
        admission.verify_loaded_sources(self.sources)
        self.runtime = engine.runtime_identity(endpoint, policy.image_id, timeout_seconds=policy.transport_timeout_seconds)
        self.browser_runtime = runtime_identity(policy)
        self.binding = binding_for(self.files, profile, policy, self.runtime, self.browser_runtime)
        require(self.tree == registration.tree_oid and self.binding == registration.binding
                and self.sources == _LOADED_SOURCES, "Source/runtime/evaluator binding differs")
        declared = registration.observation
        self.actual_registration = observation_registration_for(self.binding, profile, policy,
            subject=declared.gate.binding.subject, gate_id=declared.gate.gate_id, commit_oid=registration.commit_oid,
            tree_oid=self.tree, repetition_id=registration.repetition_id, cohort_trajectory_ids=declared.cohort_trajectory_ids)
        require(self.actual_registration == declared, "Prospective browser admission differs")
        self.retained_freeze = self.admission.before_intent(self.actual_registration)
        self.config = {"protocol": PROTOCOL, "registration": asdict(registration), "profile": profile.record(),
            "policy": asdict(policy), "sources": self.sources, "runtime": self.runtime, "browser_runtime": self.browser_runtime,
            "endpoint": asdict(endpoint), "source_manifest": source_manifest(self.files), "input_manifest": source_manifest(self.inputs),
            "owner_roots": [str(x) for x in roots], "mode": "physical", "global_independent_acceptance": False}
        self.journal = owner_journal.OwnerJournal(self.root, self.delta_root,
            context={"protocol": PROTOCOL, "config_sha256": digest(self.config),
                     "registration_sha256": digest(asdict(registration)), "purpose": self.binding.purpose}, authority=checkpoint_authority)
        try:
            self._retain("config.json", encoded(self.config))
        except BaseException:
            self.journal.close()
            raise

    def checkpoint(self) -> chain.PrefixCommitment:
        require(not self.closed, "Closed browser owner")
        return self.journal.checkpoint()

    def read_authenticated(self, name: str) -> bytes:
        require(not self.closed, "Closed browser owner")
        return self.journal.read(name)

    def _retain(self, name: str, raw: bytes) -> None:
        require(re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", name) is not None and type(raw) is bytes
                and len(raw) <= MAX_RECORD_BYTES, "Bounded immutable evidence required")
        self.journal.retain(name, raw, cleanup=self._cleanup_mode)
        require(self.journal.read(name) == raw, "Acknowledged original changed")
        if name.endswith(("-request.bin", "-command-intent.json")):
            self.checkpoint()

    def _remaining(self, requested: float) -> float:
        bound = self._history_deadline if self._cleanup_mode else self._work_deadline
        remaining = min(requested, bound - time.monotonic())
        require(remaining > 0, "Declared browser history deadline reached")
        return remaining

    def _deadline_after(self, seconds: float) -> float:
        return time.monotonic() + self._remaining(seconds)

    def _unchanged(self) -> None:
        self.checkpoint()
        self.admission.check_current(self.actual_registration, self.retained_freeze)
        require(self.sources == evaluator_sources() == _LOADED_SOURCES, "Loaded evaluator changed")
        tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files, "Registered Git source changed")
        for key, fingerprint in (("node_executable", "node_sha256"), ("browser_executable", "browser_sha256")):
            require(sha256(Path(self.browser_runtime[key]).read_bytes()) == self.browser_runtime[fingerprint], "Pinned runtime binary changed")
        if getattr(self, "_staging_active", False):
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
            "gossip.helper": self.binding.helper_sha256, "gossip.step": step_id,
            "gossip.epoch": "0" if role == "keeper" else "1"}.items()))

    def _keeper_boundary(self, label: str) -> None:
        self._boundary(label + "-keeper", self.keeper, self.keeper_spec)
        inspected = self._checked(label + "-volume", ["docker", "volume", "inspect", "--format", "{{json .}}", self.volume])
        require(engine.strict_json_loads(self._raw(inspected)) == self.volume_baseline, "Held volume changed")

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
        require(self._raw(created).strip() == self.volume.encode(), "Created volume differs")
        inspected = self._checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", self.volume])
        self.volume_baseline = engine.strict_json_loads(self._raw(inspected))
        require(self.volume_baseline.get("Name") == self.volume and self.volume_baseline.get("Driver") == "local"
                and self.volume_baseline.get("Options") == VOLUME_OPTIONS and self.volume_baseline.get("Labels") == labels,
                "Owned volume restrictions differ")
        self._retain("volume-baseline.json", encoded(self.volume_baseline))
        self.emergency_cleanup.confirm_volume(self._cleanup_claims[self.volume], baseline_record="volume-baseline.json")
        self.keeper_spec = RoleSpec("keeper", "gossip-" + self.execution_id + "-keeper",
            ("python", "-I", "-c", "import time;time.sleep(" + str(self.policy.lifetime_seconds) + ")"),
            self._labels("keeper", "state-lifetime"), (), self.volume)
        self.keeper = self._start_role("keeper", self._create("keeper", self.keeper_spec, self.owned), self.keeper_spec)
        declaration, argv = setup_argv(self.profile.case, self.keeper["Id"])
        self._retain("initial-state-intent.json", encoded({"setup": declaration, "argv": argv,
            "keeper_id": self.keeper["Id"], "input_manifest": source_manifest(self.inputs)}))
        initial = self._checked("initial-state", argv)
        require(self._raw(initial) == encoded({"protocol": base.SETUP_PROTOCOL,
            "setup_sha256": digest(declaration), "ready": True}) + b"\n" and not self._raw(initial, "stderr"),
            "Trusted initial state acknowledgment differs")
        self._keeper_boundary("initial-state-after")
        self.server_spec = RoleSpec("server", "gossip-" + self.execution_id + "-server", cases.SERVER_ARGV,
            self._labels("server", "server"), (("/workspace", str(self.workspace)), ("/inputs", str(self.inputs_root))), self.volume)
        self.server = self._start_role("server", self._create("server", self.server_spec, self.owned), self.server_spec)

    def _probe(self, message: dict[str, Any]) -> dict[str, Any]:
        require(all(row.get("removed") is True for row in self.request_rows), "A prior probe remains unretired")
        identifier = message["request_id"]
        label = "r" + str(identifier).zfill(4)
        self._retain(label + "-request.json", encoded(message))
        require(message["source"] in ("browser", "control") and type(identifier) is int
                and 1 <= identifier <= self.policy.maximum_requests, "Closed actual browser/control request required")
        recipe = bridge_request(message["request"])
        input_raw = wire.build_probe_input(recipe, cases.PORT, self.policy.probe.wire_limits)
        self._retain(label + "-probe-input.bin", input_raw)
        probe_root = self.staging / label
        probe_root.mkdir(mode=0o755)
        staged = {"helper.py": wire.helper_source(), "request.json": input_raw}
        for name, raw in staged.items():
            target = probe_root / name
            target.write_bytes(raw)
            target.chmod(0o444)
        finite._verify_tree(probe_root, staged)
        spec = RoleSpec("probe", "gossip-" + self.execution_id + "-" + label, base.PROBE_ARGV,
            self._labels("probe", label), (("/probe", str(probe_root)),), server_id=self.server["Id"])
        self._retain(label + "-staging.json", encoded({"root": str(probe_root), "manifest": source_manifest(staged)}))
        row: dict[str, Any] = {"request_id": identifier, "action_id": message["action_id"], "source": message["source"],
            "label": label, "process": None, "probe_id": None, "removed": False, "available": False}
        response: dict[str, Any] = {"available": False, "reason": "probe did not complete"}
        created: dict[str, Any] | None = None
        try:
            self._unchanged()
            self._keeper_boundary(label + "-before")
            current = self._inspect(label + "-server-before", self.server["Id"])
            self._compare(label + "-server-before-continuity", self.server, current, self.server_spec, "running-to-running")
            donor = transport.ProbeDonorEvidence(self.server_spec, self.registration, 1,
                encoded(self.server), encoded(current), encoded(self.runtime))
            created = self._create(label + "-probe", spec, self.owned)
            row["probe_id"] = created["Id"]
            process = transport.run_probe(self.endpoint, expected=created, spec=spec, policy=self.policy.probe,
                runtime=self.runtime, retain=self._retain, label=label + "-probe", donor=donor,
                history_deadline=self._work_deadline)
            row["process"] = process
            require(process["status"] == "completed" and process["completion"]["natural"]
                    and process["completion"]["inspect_exit_code"] == 0 and not self._raw(process, "stderr"),
                    "Trusted helper completion unavailable")
            observed = wire.decode_probe_output(self._raw(process), recipe, cases.PORT, self.policy.probe.wire_limits)
            row["sent_complete"] = observed.sent_complete
            row["wire_limitations"] = list(observed.limitations)
            self._boundary(label + "-server-after", self.server, self.server_spec)
            self._keeper_boundary(label + "-after")
            finite._verify_tree(probe_root, staged)
            self._unchanged()
            response = bridge_response(observed)
            row["available"] = True
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            response = {"available": False, "reason": type(error).__name__ + ":" + str(error)[:512]}
            row["limitation"] = response["reason"]
        finally:
            if created is not None:
                try:
                    row["removed"] = self._remove(label + "-retire", spec, created["Id"], force=True)
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    row["cleanup_limitation"] = type(error).__name__ + ":" + str(error)[:512]
        if not row["removed"]:
            response = {"available": False, "reason": "probe cleanup not independently confirmed"}
            row["available"] = False
        self._retain(label + "-observation.json", encoded(row))
        self.request_rows.append(row)
        return {"protocol": IPC_PROTOCOL, "kind": "response", "request_id": identifier, **response}

    def _browser(self) -> dict[str, Any]:
        """Bounded owned Node IPC. There is no caller-prepared browser receipt path."""
        driver_root = self.staging / "trusted-browser"
        driver_root.mkdir(mode=0o700)
        staged_driver_files = {}
        for source in (DRIVER, PROTOCOL_SOURCE, BASE_PROJECT / "devtools/browser/lifecycle.cjs"):
            raw = source.read_bytes()
            staged_driver_files[source.name] = raw
            target = driver_root / source.name
            target.write_bytes(raw)
            target.chmod(0o400)
            self._retain("browser-staged-" + source.name.replace("_", "-") + ".bin", raw)
        actions = self.profile.case.record["actions"]
        # Expected exact integers stay host-side decimal strings. The driver
        # never parses candidate JSON when selecting or resubmitting a token.
        driver_actions = json.loads(encoded(actions))
        for action in driver_actions:
            for job in action.get("jobs", []) + ([action["job"]] if "job" in action else []):
                job["epoch"] = str(job["epoch"])
        declaration = {"protocol": IPC_PROTOCOL, "runtime": self.browser_runtime, "actions": driver_actions}
        input_path, output = driver_root / "input.json", driver_root / "output"
        input_path.write_bytes(encoded(declaration))
        argv = [self.policy.node_executable, str(driver_root / DRIVER.name), "--input", str(input_path), "--output", str(output)]
        environment = {"PATH": os.environ.get("PATH", ""), "NODE_PATH": self.policy.node_modules_path}
        self._retain("browser-command-intent.json", encoded({"argv": argv, "environment": environment,
            "declaration": declaration, "driver_sha256": sha256(DRIVER.read_bytes()),
            "protocol_sha256": sha256(PROTOCOL_SOURCE.read_bytes()), "start_new_session": True,
            "candidate_mount": False, "deadline_monotonic": self._work_deadline}))
        child: subprocess.Popen[bytes] | None = None
        selector = selectors.DefaultSelector()
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        errors: list[str] = []
        sequence, request_id, next_action = 0, 0, 0
        terminal: dict[str, Any] | None = None
        try:
            child = subprocess.Popen(argv, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True)
            self._retain("browser-start.json", encoded({"pid": child.pid, "argv": argv,
                "started_monotonic_ns": time.monotonic_ns(), "owner_pid": os.getpid()}))
            assert child.stdout is not None and child.stderr is not None and child.stdin is not None
            for kind in buffers:
                pipe = getattr(child, kind)
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, kind)
            os.set_blocking(child.stdin.fileno(), False)
            while selector.get_map():
                self._remaining(1)
                for key, _ in selector.select(timeout=min(0.1, self._remaining(1))):
                    chunk = os.read(key.fd, 65536)
                    kind = key.data
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffers[kind].extend(chunk)
                    require(len(buffers[kind]) <= self.policy.message_limit, "Bounded browser pipe allocation exhausted")
                    if kind == "stderr":
                        continue
                    while b"\n" in buffers[kind]:
                        line, _, rest = buffers[kind].partition(b"\n")
                        buffers[kind] = bytearray(rest)
                        raw = bytes(line)
                        sequence += 1
                        self._retain("browser-message-" + str(sequence).zfill(5) + ".bin", raw)
                        message = engine.strict_json_loads(raw)
                        require(type(message) is dict and message.get("protocol") == IPC_PROTOCOL and message.get("seq") == sequence,
                                "Browser pipe identity/sequence differs")
                        self.driver_messages.append(sequence)
                        message_kind = message.get("kind")
                        if message_kind == "request":
                            request_id += 1
                            require(message["request_id"] == request_id and terminal is None
                                    and message["action_id"] in {action["id"] for action in actions[:next_action]},
                                    "Browser request order/action differs")
                            self.request_intents.append({"request_id": request_id, "action_id": message["action_id"], "source": message["source"]})
                            reply = self._probe(message)
                            reply_raw = encoded(reply) + b"\n"
                            self._retain("browser-reply-" + str(request_id).zfill(4) + ".bin", reply_raw)
                            view = memoryview(reply_raw)
                            while view:
                                self._remaining(1)
                                try:
                                    written = os.write(child.stdin.fileno(), view)
                                    require(written > 0, "Browser input pipe stopped")
                                    view = view[written:]
                                except BlockingIOError:
                                    # An owned local pipe only; no candidate process runs on host.
                                    import select
                                    select.select([], [child.stdin.fileno()], [], min(0.1, self._remaining(1)))
                        elif message_kind == "action_start":
                            require(next_action < len(actions) and message["action_id"] == actions[next_action]["id"]
                                    and terminal is None, "Closed ordered browser action differs")
                            next_action += 1
                        elif message_kind == "observation":
                            require(next_action > 0 and message["action_id"] == actions[next_action - 1]["id"], "Observation action differs")
                        elif message_kind == "runtime":
                            require(sequence == 1 and message["runtime"] == self.browser_runtime
                                    and message["driver_sha256"] == sha256(DRIVER.read_bytes()), "Actual driver/runtime differs")
                        elif message_kind == "launched":
                            require(message["browser_version"] == self.browser_runtime["browser_version"]
                                    and message["browser_launches"] == 1, "Actual browser launch identity differs")
                        elif message_kind == "terminal":
                            require(terminal is None, "Repeated browser terminal")
                            terminal = message
                        else:
                            raise ExecutionError("Unknown browser message")
            require(not buffers["stdout"], "Incomplete browser stdout message")
            child.wait(timeout=self._remaining(15))
        except BaseException as error:
            errors.append(type(error).__name__ + ":" + str(error)[:512])
        finally:
            try:
                selector.close()
            except BaseException as error:
                errors.append("selector-close:" + type(error).__name__)
            if child is not None:
                if child.poll() is None:
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                        child.wait(timeout=min(10, max(0.1, self._history_deadline - time.monotonic())))
                    except (OSError, subprocess.TimeoutExpired):
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.wait(timeout=min(5, max(0.1, self._history_deadline - time.monotonic())))
                        except (OSError, subprocess.TimeoutExpired) as error:
                            errors.append("driver-reap-unproven:" + type(error).__name__)
                for pipe in (child.stdin, child.stdout, child.stderr):
                    if pipe is not None:
                        try:
                            pipe.close()
                        except BaseException as error:
                            errors.append("driver-pipe-close:" + type(error).__name__)
            self._retain("browser-stderr.bin", bytes(buffers["stderr"]))
            if buffers["stdout"]:
                self._retain("browser-incomplete-stdout.bin", bytes(buffers["stdout"]))
        staged_source_unchanged = True
        for name, expected in staged_driver_files.items():
            try:
                actual = (driver_root / name).read_bytes()
                require(len(actual) <= 1024 * 1024, "Staged trusted driver file oversized")
                self._retain("browser-final-" + name.replace("_", "-") + ".bin", actual)
                staged_source_unchanged = staged_source_unchanged and actual == expected
            except (OSError, ValueError) as error:
                staged_source_unchanged = False
                errors.append("driver-source:" + type(error).__name__)
        core_result = {"staged_source_unchanged": staged_source_unchanged, "pid": None if child is None else child.pid, "returncode": None if child is None else child.returncode,
            "messages": sequence, "requests": request_id, "started_actions": next_action, "terminal": terminal,
            "errors": list(errors), "pipe_complete": not errors and not buffers["stdout"],
            "browser_close_acknowledged": terminal is not None and terminal.get("cleanup", {}).get("browser") == "complete"
                and not terminal.get("force_exit_needed")}
        # Original process facts precede optional artifact allocation/retention.
        # Later screenshot/trace loss must not erase a known browser request.
        self._retain("browser-process-completion.json", encoded(core_result))
        artifact_rows = []
        if terminal is not None:
            for name in terminal.get("artifacts", []):
                require(type(name) is str and re.fullmatch(r"context-[0-9]{2}\.(png|zip)", name) is not None,
                        "Untrusted artifact path")
                path = output / name
                limit = 16 * 1024 * 1024 if name.endswith(".png") else 64 * 1024 * 1024
                require(path.is_file() and not path.is_symlink() and path.stat().st_size <= limit, "Browser artifact unavailable/oversized")
                raw = path.read_bytes()
                parts = []
                for index in range(0, len(raw), 8 * 1024 * 1024):
                    retained = "browser-artifact-" + name.replace(".", "-") + "-" + str(index // (8 * 1024 * 1024)).zfill(3) + ".bin"
                    part = raw[index:index + 8 * 1024 * 1024]
                    self._retain(retained, part)
                    parts.append({"path": retained, "sha256": sha256(part), "bytes": len(part)})
                artifact_rows.append({"name": name, "sha256": sha256(raw), "bytes": len(raw), "parts": parts})
        result = {**core_result, "artifacts": artifact_rows}
        self._retain("browser-completion.json", encoded(result))
        return result

    def execute_once(self) -> chain.PrefixCommitment:
        self._unchanged()
        require(not self.journal.has("intent.json"), "Never redispatch a retained browser intent")
        self.execution_id = "browser-" + uuid.uuid4().hex
        self.volume = "gossip-" + self.execution_id + "-volume"
        self.owned: dict[str, tuple[RoleSpec, str | None]] = {}
        self.request_rows: list[dict[str, Any]] = []
        self.request_intents: list[dict[str, Any]] = []
        self.driver_messages: list[int] = []
        self._retain("intent.json", encoded({"protocol": PROTOCOL, "execution_id": self.execution_id, "volume": self.volume,
            "binding_sha256": digest(asdict(self.binding)), "profile_sha256": self.profile.sha256,
            "ordered_actions": self.profile.case.record["actions"], "no_automatic_retry": True, "physical": True}))
        infrastructure: list[str] = []
        browser: dict[str, Any] | None = None
        cleanup_verified = False
        with tempfile.TemporaryDirectory(prefix="gossip-browser-stage-") as temporary:
            self._staging_active = True
            self.staging = Path(temporary).resolve()
            self.staging.chmod(0o755)
            self.workspace, self.inputs_root = self.staging / "workspace", self.staging / "inputs"
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
                candidate_mount_roots=(self.staging,), journal_roots=(self.root, self.delta_root, self.checkpoint_authority.root))
            self._retain("staging.json", encoded({"workspace": str(self.workspace), "inputs": str(self.inputs_root),
                "source_manifest": source_manifest(self.files), "input_manifest": source_manifest(self.inputs)}))
            try:
                self._setup()
                browser = self._browser()
                self._boundary("final-server", self.server, self.server_spec)
                self._keeper_boundary("final-state")
                self._unchanged()
                require(runtime_identity(self.policy) == self.browser_runtime, "Final installed browser/runtime changed")
                require(engine.runtime_identity(self.endpoint, self.policy.image_id,
                    timeout_seconds=self._remaining(self.policy.transport_timeout_seconds)) == self.runtime, "Final Engine runtime changed")
            except BaseException as error:
                infrastructure.append(type(error).__name__ + ":" + str(error)[:512])
            finally:
                self._cleanup_mode = True
                if self.emergency_cleanup is not None:
                    try:
                        result = self.emergency_cleanup.run(reason="browser history closed")
                        if not self.journal.uncertain:
                            self._retain("cleanup-result.json", encoded(asdict(result)))
                        cleanup_verified = result.all_resources_absent
                        infrastructure.extend(result.uncertainties)
                    except BaseException as error:
                        infrastructure.append("emergency-cleanup:" + type(error).__name__ + ":" + str(error)[:512])
            require(not self.journal.uncertain, "Uncertain browser original journal; separate cleanup remains")
            self._retain("terminal.json", encoded({"protocol": PROTOCOL, "execution_id": self.execution_id,
                "planned_actions": len(self.profile.case.record["actions"]), "driver_messages": self.driver_messages,
                "request_rows": self.request_rows, "request_intents": self.request_intents,
                "planned_control_requests": 2 * len(self.profile.case.record["actions"]),
                "maximum_combined_requests": self.policy.maximum_requests,
                "observed_probe_records": len(self.request_rows),
                "authenticated_sent_requests": sum(row.get("sent_complete") is True for row in self.request_rows),
                "unknown_request_outcomes": len(self.request_intents) - sum(row.get("sent_complete") is True for row in self.request_rows),
                "created_resource_ids": {name: cid for name, (_, cid) in self.owned.items() if cid is not None},
                "unconfirmed_create_names": [name for name, (_, cid) in self.owned.items() if cid is None],
                "browser": browser, "cleanup_verified": cleanup_verified,
                "infrastructure": infrastructure, "purpose": self.binding.purpose, "product_acceptance": False,
                "global_independent_acceptance": False}))
        self._staging_active = False
        return self.checkpoint()

    def close(self) -> None:
        if self.closed:
            return
        errors: list[BaseException] = []
        closers = []
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

    def __enter__(self) -> BrowserExecution:
        require(not self.closed, "Closed owner")
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def setup_argv(case: cases.BrowserCase, keeper_id: str) -> tuple[dict[str, Any], list[str]]:
    require(_SHA.fullmatch(keeper_id) is not None, "Exact keeper identity required")
    raw = case.inputs().get("initial.sqlite", b"")
    seed = {"fixture_path": "initial.sqlite", "bytes": len(raw), "sha256": sha256(raw)} if raw else None
    value = {"protocol": base.SETUP_PROTOCOL, "database_path": "/tmp/library.sqlite", "directories": list(base.SETUP_DIRECTORIES),
             "seed": seed, "bootstrap_sha256": sha256(base.SETUP_SOURCE.encode()), "candidate_code_executed": False}
    data = base64.b64encode(raw).decode()
    return value, ["docker", "exec", "--user=65534:65534", keeper_id, "python", "-I", "-c", base.SETUP_SOURCE,
                   encoded(value).decode(), *(data[index:index + 32768] for index in range(0, len(data), 32768))]


_LOADED_SOURCES = evaluator_sources()
