"""Explicit browser-family fork of the independently inspected probe donor.

The prior closed product-process executor is unchanged. Wire parsing, role
restrictions and Engine evidence APIs retain their existing implementation.
This version only admits the browser registration; it cannot authenticate an old
profile or confer product credit by itself.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass
from pathlib import Path
import socket
import threading
import time
from typing import TYPE_CHECKING, Any, Callable
from . import candidate_product_process_execution_v1 as base
from . import candidate_http_transport_v1 as wire
from . import candidate_client_process_v4 as engine
if TYPE_CHECKING:
    from .candidate_product_browser_execution_v1 import BrowserRegistration
PROTOCOL = "candidate-product-browser-probe-v1"
ROLE_POLICY = "candidate-product-browser-distinct-probe-donor-v1"
RoleSpec = base.RoleSpec
HttpPolicy = base.HttpPolicy
require = base.require
encoded = base.encoded
sha256 = base.sha256
digest = base.digest
validate_role = base.validate_role
validate_state = base.validate_state
ExecutionError = base.ExecutionError


def role_policy_identity() -> dict[str, Any]:
    return {"policy": ROLE_POLICY, "protocol": PROTOCOL, "mechanical_base": base.role_policy_identity(),
            "source_sha256": sha256(Path(__file__).read_bytes()),
            "registration": "candidate-product-browser-registration-v1"}


def role_identity_comparison(before: dict[str, Any], after: dict[str, Any], spec: RoleSpec,
                             runtime: dict[str, Any], phase: str, *,
                             donor: ProbeDonorEvidence | None = None) -> dict[str, Any]:
    """Permit only declared typed transitions, preserving complete raw evidence."""
    require(phase in ("created-to-prestart", "created-to-running", "created-to-exited",
                      "running-to-running", "running-to-exited"), "Unqualified role transition")
    require(donor is None or (spec.role == "probe" and phase == "created-to-exited"),
            "Donor authority is restricted to probe created-to-exited")
    validate_role(before, spec, runtime["image_id"], runtime)
    validate_role(after, spec, runtime["image_id"], runtime)
    left, right = (engine.strict_json_loads(encoded(engine.immutable_inspection(x))) for x in (before, after))
    transformations: list[str] = []
    for value in (left, right):
        value["Mounts"] = {x["Destination"]: x for x in value["Mounts"]}
    old, new = left["HostConfig"]["OomKillDisable"], right["HostConfig"]["OomKillDisable"]
    if old is False and new is None and phase in ("created-to-running", "created-to-exited"):
        right["HostConfig"]["OomKillDisable"] = False
        transformations.append("pinned-runtime-startup-oom-false-to-null")
    else:
        require(old is new, "Unqualified OOM default transition")
    changed_fields: list[dict[str, Any]] = []
    if spec.role == "probe" and phase == "created-to-exited":
        require(type(donor) is ProbeDonorEvidence, "Independent current-server donor evidence required")
        assert donor is not None
        donor.validate_probe(spec, runtime)
        validate_state(before, "created")
        validate_state(after, "exited")
        require(before["Id"] == after["Id"] and before["Id"] != donor.server_id
                and left["Config"]["Hostname"] == before["Id"][:12]
                and right["Config"]["Hostname"] == donor.hostname,
                "Probe hostname does not follow independently bound donor startup")
        changed_fields.append({"path": "Config.Hostname", "before": left["Config"]["Hostname"],
            "after": right["Config"]["Hostname"], "comparison_after": left["Config"]["Hostname"],
            "donor_inspection_sha256": digest(donor.inspection), "donor_record_sha256": digest(donor.record())})
        right["Config"]["Hostname"] = left["Config"]["Hostname"]
        transformations.append("pinned-runtime-probe-donor-hostname")
    matches = encoded(left) == encoded(right)
    if phase.startswith("running-"):
        matches = matches and encoded(before["State"].get("StartedAt")) == encoded(after["State"].get("StartedAt"))
        if phase == "running-to-running":
            matches = matches and type(before["State"].get("Pid")) is int and before["State"]["Pid"] > 0 \
                and before["State"]["Pid"] == after["State"].get("Pid")
    return {"policy": ROLE_POLICY, "role": spec.role, "phase": phase, "matches": bool(matches),
            "before_sha256": digest(before), "after_sha256": digest(after), "runtime_sha256": digest(runtime),
            "transformations": transformations, "changed_fields": changed_fields,
            "policy_identity": role_policy_identity(),
            "donor_sha256": digest(donor.record()) if donor is not None else None,
            "comparison_sha256": digest([left, right])}


@dataclass(frozen=True)
class ProbeDonorEvidence:
    """Immutable complete independently observed donor; hashes alone confer no authority."""
    server_spec: RoleSpec
    registration: BrowserRegistration
    epoch: int
    baseline_json: bytes
    inspection_json: bytes
    runtime_json: bytes

    def __post_init__(self) -> None:
        from .candidate_product_browser_execution_v1 import BrowserRegistration
        require(type(self.server_spec) is RoleSpec and self.server_spec.role == "server"
                and type(self.registration) is BrowserRegistration and type(self.epoch) is int
                and self.epoch == 1, "Typed source-bound server donor required")
        for raw in (self.baseline_json, self.inspection_json, self.runtime_json):
            require(type(raw) is bytes, "Canonical complete donor evidence required")
            value = engine.strict_json_loads(raw)
            require(type(value) is dict and encoded(value) == raw, "Canonical complete donor evidence required")
        baseline = engine.strict_json_loads(self.baseline_json)
        current, runtime = self.inspection, self.runtime
        binding = self.registration.binding
        require(digest(runtime) == binding.engine_runtime_sha256
                and binding.role_policy_sha256 == digest(role_policy_identity()),
                "Donor runtime or policy differs from registration")
        labels = dict(self.server_spec.labels)
        require(labels.get("gossip.role") == "server" and labels.get("gossip.source") == binding.source_sha256
                and labels.get("gossip.fixture") == binding.fixture_sha256
                and labels.get("gossip.helper") == binding.helper_sha256
                and labels.get("gossip.epoch") == str(self.epoch)
                and type(labels.get("gossip.execution")) is str and bool(labels["gossip.execution"]),
                "Donor source, fixture or current epoch differs")
        for value in (baseline, current):
            validate_role(value, self.server_spec, runtime["image_id"], runtime)
            validate_state(value, "running")
        require(role_identity_comparison(baseline, current, self.server_spec, runtime,
                    "running-to-running")["matches"] is True, "Donor current running continuity differs")

    @property
    def inspection(self) -> dict[str, Any]:
        return engine.strict_json_loads(self.inspection_json)

    @property
    def runtime(self) -> dict[str, Any]:
        return engine.strict_json_loads(self.runtime_json)

    @property
    def server_id(self) -> str:
        return str(self.inspection["Id"])

    @property
    def hostname(self) -> str:
        return str(self.inspection["Config"]["Hostname"])

    def validate_probe(self, spec: RoleSpec, runtime: dict[str, Any]) -> None:
        # Revalidate full immutable evidence, never merely compare caller-supplied hashes.
        self.__post_init__()
        require(type(spec) is RoleSpec and spec.role == "probe" and spec.server_id == self.server_id
                and encoded(runtime) == self.runtime_json, "Probe donor ID, role or runtime differs")
        server_labels, probe_labels = dict(self.server_spec.labels), dict(spec.labels)
        require(probe_labels.get("gossip.role") == "probe" and all(probe_labels.get(key) == server_labels[key]
                for key in ("gossip.execution", "gossip.source", "gossip.fixture", "gossip.epoch", "gossip.helper")),
                "Probe donor source or current epoch differs")

    def record(self) -> dict[str, Any]:
        return {"protocol": "candidate-product-browser-probe-donor-v1", "policy": role_policy_identity(),
            "server_id": self.server_id, "epoch": self.epoch, "hostname": self.hostname,
            "domainname": self.inspection["Config"]["Domainname"], "inspection_sha256": sha256(self.inspection_json),
            "baseline_sha256": sha256(self.baseline_json), "runtime_sha256": sha256(self.runtime_json),
            "binding": asdict(self.registration.binding), "commit_oid": self.registration.commit_oid,
            "tree_oid": self.registration.tree_oid, "server_spec": asdict(self.server_spec),
            "baseline": engine.strict_json_loads(self.baseline_json), "inspection": self.inspection, "runtime": self.runtime}


def run_probe(endpoint: engine.EngineEndpoint, *, expected: dict[str, Any], spec: RoleSpec,
              policy: HttpPolicy, runtime: dict[str, Any], retain: Callable[[str, bytes], None],
              label: str, donor: ProbeDonorEvidence, history_deadline: float | None = None,
              execution_protocol: str = PROTOCOL) -> dict[str, Any]:
    """A distinct finite trusted role, authenticated by raw Engine attach/wait/inspect."""
    def bounded(seconds: float) -> float:
        deadline = time.monotonic() + seconds
        if history_deadline is not None:
            deadline = min(deadline, history_deadline)
        require(deadline > time.monotonic(), "History observation deadline exhausted")
        return deadline
    require(spec.role == "probe", "Only the fixed probe has finite completion authority")
    require(execution_protocol == PROTOCOL, "Closed HTTP execution protocol required")
    validate_role(expected, spec, policy.image_id, runtime)
    validate_state(expected, "created")
    require(type(donor) is ProbeDonorEvidence, "Typed precreation donor required")
    donor.validate_probe(spec, runtime)
    limit = wire.max_probe_output_bytes(policy.wire_limits)
    require(limit <= policy.stream_limit_bytes, "Trusted transcript exceeds declared process capture")
    process_policy = engine.ProcessPolicy(policy.image_id, timeout_seconds=policy.probe_timeout_seconds,
        stream_limit_bytes=limit, frame_limit_bytes=limit, transport_timeout_seconds=policy.transport_timeout_seconds)
    decoder = engine.MultiplexDecoder(process_policy)
    prefix = "/containers/" + expected["Id"]
    raw_wire: engine._Wire | None = None
    reader: threading.Thread | None = None
    errors: list[BaseException] = []
    started = False
    eof_after_start: bool | None = None
    final: dict[str, Any] = {}
    waited: dict[str, Any] = {}
    comparison: dict[str, Any] | None = None
    start_response: dict[str, Any] | None = None
    evidence: dict[str, dict[str, Any]] = {}
    retention_failure: BaseException | None = None
    def save(name: str, raw: bytes) -> None:
        nonlocal retention_failure
        if retention_failure is not None:
            raise retention_failure
        path = label + "-" + name
        require(name not in evidence, "Repeated probe evidence")
        try:
            retain(path, raw)
        except BaseException as error:
            retention_failure = error
            raise
        evidence[name] = {"path": path, "bytes": len(raw), "sha256": sha256(raw)}
    def response_observed(code: int) -> None:
        nonlocal started
        if code == 204:
            started = True
    def drain() -> None:
        nonlocal eof_after_start
        assert raw_wire is not None
        try:
            while True:
                if raw_wire.buffer:
                    chunk = bytes(raw_wire.buffer)
                    raw_wire.buffer.clear()
                    decoder.feed(chunk)
                if raw_wire.eof:
                    eof_after_start = started
                    require(eof_after_start is True, "Probe attach EOF preceded start acknowledgement")
                    decoder.eof()
                    return
                raw_wire.receive()
        except (OSError, ValueError) as error:
            errors.append(error)
    status = "completion_unproven"
    error_text: str | None = None
    try:
        save("intent.json", encoded({"protocol": execution_protocol, "role": asdict(spec), "expected": digest(expected),
            "runtime": runtime, "policy": asdict(policy), "role_policy": role_policy_identity(),
            "donor": donor.record(), "helper_stdout_envelope": limit,
            "helper_sha256": wire.helper_sha256()}))
        before = engine._json_control(endpoint, prefix + "/json", deadline=bounded(policy.transport_timeout_seconds),
                                      retain=save, label="prestart")
        validate_state(before, "created")
        prestart = role_identity_comparison(expected, before, spec, runtime, "created-to-prestart")
        save("prestart-comparison.json", encoded(prestart))
        require(prestart["matches"], "Probe changed before start")
        request = engine._request("POST", prefix + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True)
        save("attach-request.bin", request)
        raw_wire = engine._Wire(endpoint, bounded(policy.transport_timeout_seconds),
            engine.HEADER_LIMIT + 2 * limit + engine.FRAME_COUNT_LIMIT * 8)
        raw_wire.send(request)
        code, headers = raw_wire.headers()
        require(code == 101 and headers.get("connection", "").lower() == "upgrade"
                and headers.get("upgrade", "").lower() == "tcp"
                and headers.get("content-type") in ("application/vnd.docker.raw-stream", "application/vnd.docker.multiplexed-stream")
                and "content-length" not in headers and "transfer-encoding" not in headers and not raw_wire.buffer,
                "Probe raw attachment failed")
        raw_wire.socket.settimeout(0)
        try:
            raw_wire.socket.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            pass
        else:
            raise ExecutionError("Probe attachment ended or produced data before start")
        finally:
            raw_wire._timeout()
        deadline = bounded(policy.probe_timeout_seconds)
        raw_wire.deadline = deadline
        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        require(not errors and not decoder.complete, "Probe attachment failed before dispatch")
        code, body = engine._control(endpoint, "POST", prefix + "/start", deadline=deadline,
            retain=save, label="start", on_response=response_observed)
        start_response = {"status": code, "body_bytes": len(body), "body_sha256": sha256(body),
            "framing_complete": True, "eof_observed": True,
            "request": evidence["start-request.bin"], "response": evidence["start-response.bin"]}
        save("start-completion.json", encoded(start_response))
        require(code == 204, "Probe start rejected")
        waited = engine._json_control(endpoint, prefix + "/wait?condition=not-running", deadline=deadline,
                                       retain=save, label="wait", method="POST")
        final = engine._json_control(endpoint, prefix + "/json", deadline=bounded(policy.transport_timeout_seconds),
                                    retain=save, label="final")
        comparison = role_identity_comparison(expected, final, spec, runtime, "created-to-exited", donor=donor)
        save("identity-comparison.json", encoded(comparison))
        reader.join(timeout=max(0, deadline - time.monotonic()))
        require(not reader.is_alive() and not errors and decoder.complete, "Probe complete attach EOF unproven")
        completed = engine.completion_evidence(waited, final, started=started, killed=False,
                                               identity_ok=comparison["matches"] is True)
        require(completed["natural"] and completed["inspect_exit_code"] == 0, "Trusted helper did not complete normally")
        status = "completed"
    except (OSError, ValueError) as error:
        error_text = type(error).__name__ + ":" + str(error)[:512]
    finally:
        if raw_wire is not None:
            try:
                raw_wire.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            if reader is not None and reader.ident is not None:
                reader.join(timeout=max(0, min(policy.transport_timeout_seconds,
                    (history_deadline - time.monotonic()) if history_deadline is not None else policy.transport_timeout_seconds)))
                if reader.is_alive():
                    status, error_text = "completion_unproven", "Probe reader did not stop"
            try:
                save("attach-response.bin", bytes(raw_wire.raw))
            finally:
                raw_wire.close()
    result: dict[str, Any] = {"protocol": execution_protocol, "role": "probe", "status": status, "error": error_text,
        "container_id": expected["Id"], "start_response": start_response,
        "role_policy": role_policy_identity(), "donor": donor.record(),
        "completion": engine.completion_evidence(waited, final, started=started, killed=False,
            identity_ok=comparison is not None and comparison["matches"] is True),
        "identity_comparison": comparison, "attach_eof_after_start_confirmation": eof_after_start,
        "capture_complete": decoder.complete and not errors, "helper_stdout_envelope": limit}
    for kind, data in decoder.streams.items():
        save(kind + ".bin", bytes(data))
        result[kind] = {**evidence[kind + ".bin"], "observed_bytes": decoder.counts[kind],
            "truncated": decoder.counts[kind] > len(data), "complete": decoder.complete and not errors}
    result["evidence"] = dict(evidence)
    save("process.json", encoded(result))
    return result
