"""Read authenticated, fresh v4 HTTP journals into immutable semantic facts.

Only a live trusted controller owner and its independently retained checkpoint
enter this API. Historical qualification uploads and caller verdicts do not.
The original controller supplies temporal authority (including observed EOF);
replaying retained bytes is not another physical execution or an external audit.
This reader supplies facts, not acceptance, scope, promotion or cohort authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from pathlib import Path
from typing import Any

from . import candidate_http_execution_v4 as execution
from . import candidate_client_process_v4 as engine
from . import candidate_http_transport_v1 as wire
from . import candidate_http_semantics_v1 as semantics
from . import candidate_http_head_v1 as head
from . import candidate_observation_admission_v1 as admission
from . import candidate_checkpoint_chain_v1 as chain

PROTOCOL = "candidate-http-observation-v2-compact-v1"


class ObservationError(ValueError):
    """Original ownership, registration, checkpoint or raw proof is invalid."""


def require(value: bool, message: str) -> None:
    if not value:
        raise ObservationError(message)


def same(left: Any, right: Any, message: str) -> None:
    require(execution.encoded(left) == execution.encoded(right), message)


@dataclass(frozen=True)
class StepObservation:
    step_id: str
    step_index: int
    kind: str
    state: str
    facts: semantics.ResponseFacts | None
    stdout: bytes | None
    stderr: bytes | None
    exit_code: int | None
    wire_observation: wire.WireObservation | None
    limitations: tuple[str, ...]
    provenance: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class HistoryObservation:
    execution_id: str
    original_binding_sha256: str
    terminal_sha256: str
    checkpoint_sha256: str
    steps: tuple[StepObservation, ...]
    missing_step_ids: tuple[str, ...]
    cleanup_verified: bool
    infrastructure: tuple[str, ...]


class _RetainedWire(engine._Wire):
    """Strict parser over originals; receive() is parser exhaustion, not EOF proof."""
    def __init__(self, raw: bytes):
        require(len(raw) <= execution.MAX_RECORD_BYTES, "Retained response exceeds bound")
        self.buffer = bytearray(raw)
        self.eof = False

    def receive(self) -> None:
        self.eof = True


class _Reader:
    def __init__(self, owner: execution.CandidateHttpExecution,
                 checkpoint: execution.ControllerCheckpoint):
        self.owner = owner
        self.checkpoint = checkpoint
        self.validate_checkpoint()

    def validate_checkpoint(self) -> None:
        try:
            actual = self.owner.checkpoint()
        except (chain.ChainUnknown, execution.ExecutionUnknown):
            raise
        except ValueError as error:
            raise ObservationError("Original checkpoint is invalid") from error
        require(actual == self.checkpoint, "Checkpoint rollback, append or substitution")

    def raw(self, name: str) -> bytes:
        require(type(name) is str and re.fullmatch(r"[a-z][a-z0-9.-]*", name) is not None,
                "Invalid original artifact name")
        try:
            value = self.owner.read_authenticated(name)
        except (chain.ChainUnknown, execution.ExecutionUnknown):
            raise
        except ValueError as error:
            raise ObservationError("Original artifact authentication failed") from error
        require(type(value) is bytes, "Authenticated original bytes required")
        return value

    def json(self, name: str) -> dict[str, Any]:
        value = engine.strict_json_loads(self.raw(name))
        require(type(value) is dict, "Original object required")
        return value

    def descriptor(self, value: dict[str, Any]) -> bytes:
        require(type(value) is dict, "Original descriptor required")
        raw = self.raw(value["path"])
        require(type(value["bytes"]) is int and len(raw) == value["bytes"]
                and execution.sha256(raw) == value["sha256"], "Original descriptor differs")
        return raw

    def control(self, label: str, cid: str, operation: str = "json", method: str = "GET",
                status: int = 200) -> bytes:
        require(self.raw(label + "-request.bin") == engine._request(method, "/containers/" + cid + "/" + operation),
                "Original Engine request targets another operation or process")
        retained = _RetainedWire(self.raw(label + "-response.bin"))
        code, headers = retained.headers()
        require(code == status, "Original Engine response status differs")
        return retained.body(code, headers)

    def inspect(self, label: str, cid: str) -> dict[str, Any]:
        value = engine.strict_json_loads(self.control(label, cid))
        require(type(value) is dict and value.get("Id") == cid, "Original inspection full ID differs")
        return value

    def command(self, label: str, argv: list[str]) -> bytes:
        record = self.json(label + ".json")
        endpoint = self.owner.runtime["endpoint"]
        same(record["argv"], ["docker", "--host", "unix://" + endpoint["socket_path"], *argv[1:]],
             "Original Docker command differs")
        require(type(record["exit_code"]) is int and record["exit_code"] == 0
                and record["timed_out"] is False and record["capture_complete"] is True,
                "Original Docker command is incomplete")
        for key in ("stdout", "stderr"):
            raw = self.descriptor(record[key])
            require(record[key]["truncated"] is False and record[key]["observed_bytes"] == len(raw),
                    "Original Docker command capture incomplete")
        return self.descriptor(record["stdout"])

    def compare(self, label: str, before: dict[str, Any], after: dict[str, Any],
                spec: execution.RoleSpec, phase: str,
                donor: execution.ProbeDonorEvidence | None = None) -> dict[str, Any]:
        value = execution.role_identity_comparison(before, after, spec, self.owner.runtime, phase, donor=donor)
        require(value["matches"] is True, "Raw role identity differs")
        same(self.json(label + ".json"), value, "Retained role comparison differs from raw inspections")
        return value

    def boundary(self, label: str, baseline: dict[str, Any], spec: execution.RoleSpec) -> dict[str, Any]:
        current = self.inspect(label, baseline["Id"])
        execution.validate_state(current, "running")
        return self.compare(label + "-continuity", baseline, current, spec, "running-to-running")


def _spec(reader: _Reader, intent: dict[str, Any], index: int | None,
          role: str, server_id: str = "") -> execution.RoleSpec:
    owner = reader.owner
    if role == "keeper":
        return execution.RoleSpec("keeper", intent["keeper"],
            ("python", "-I", "-c", "import time;time.sleep(" + str(owner.policy.lifetime_seconds) + ")"),
            owner._labels(intent, "keeper", 0, "state-lifetime"), (), intent["volume"])
    assert index is not None
    step = owner.recipe.steps[index]
    staging = reader.json("staging.json")
    binds: tuple[tuple[str, str], ...] = (("/workspace", staging["workspace"]), ("/inputs", staging["inputs"]))
    argv = step.argv
    volume: str = intent["volume"]
    if role == "probe":
        # Probe input belongs to this exact step; no candidate-supplied helper.
        probe_root = str(Path(staging["workspace"]).parent / ("step-" + str(index).zfill(3)))
        binds, argv, volume = (("/probe", probe_root),), execution.PROBE_ARGV, ""
    return execution.RoleSpec(role, intent["containers"][index], argv,
        owner._labels(intent, role, step.epoch, step.step_id), binds, volume, server_id)


def _created(reader: _Reader, label: str, spec: execution.RoleSpec) -> dict[str, Any]:
    expected_argv = execution.create_argv(spec, reader.owner.policy.image_id)
    same(reader.json(label + "-create-intent.json"), {"spec": asdict(spec), "create_argv": expected_argv},
         "Original role creation differs from registered history")
    cid = reader.command(label + "-create", expected_argv).strip().decode("ascii")
    require(re.fullmatch(r"[0-9a-f]{64}", cid) is not None, "Full created ID required")
    result = reader.inspect(label + "-created", cid)
    execution.validate_role(result, spec, reader.owner.policy.image_id, reader.owner.runtime)
    execution.validate_state(result, "created")
    return result


def _long_role(reader: _Reader, label: str, spec: execution.RoleSpec) -> dict[str, Any]:
    created = _created(reader, label, spec)
    cid = created["Id"]
    before = reader.inspect(label + "-prestart", cid)
    execution.validate_state(before, "created")
    reader.compare(label + "-prestart-comparison", created, before, spec, "created-to-prestart")
    same(reader.json(label + "-start-intent.json"), {"container_id": cid, "spec": asdict(spec),
        "prestart_sha256": execution.digest(before), "controller_action": "start-long-lived-role"},
        "Long-lived start intent differs")
    body = reader.control(label + "-start", cid, "start", "POST", 204)
    same(reader.json(label + "-start-completion.json"), {"status": 204, "body_sha256": execution.sha256(body),
        "framing_complete": True, "eof_observed": True, "container_id": cid}, "Original long-lived start event differs")
    running = reader.inspect(label + "-running", cid)
    execution.validate_state(running, "running")
    reader.compare(label + "-startup-comparison", created, running, spec, "created-to-running")
    same(reader.json(label + "-running.json"), running, "Original running copy differs")
    return running


def _streams(reader: _Reader, label: str, cid: str, process: dict[str, Any],
             policy: engine.ProcessPolicy) -> tuple[bytes, bytes]:
    require(reader.raw(label + "-attach-request.bin") == engine._request("POST", "/containers/" + cid
        + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True), "Original attach request differs")
    retained = _RetainedWire(reader.raw(label + "-attach-response.bin"))
    code, headers = retained.headers()
    require(code == 101 and headers.get("connection", "").lower() == "upgrade"
            and headers.get("upgrade", "").lower() == "tcp"
            and headers.get("content-type") in ("application/vnd.docker.raw-stream", "application/vnd.docker.multiplexed-stream")
            and "content-length" not in headers and "transfer-encoding" not in headers,
            "Original attach upgrade differs")
    require(process["capture_complete"] is True and process["attach_eof_after_start_confirmation"] is True,
            "Original trusted capture/EOF event unavailable")
    decoder = engine.MultiplexDecoder(policy)
    decoder.feed(bytes(retained.buffer))
    decoder.eof()
    output = []
    for channel in ("stdout", "stderr"):
        item = process[channel]
        raw = reader.descriptor(item)
        require(raw == bytes(decoder.streams[channel]) and item["observed_bytes"] == decoder.counts[channel]
                and item["truncated"] is False and item["complete"] is True,
                "Original multiplex frames and stream descriptor differ")
        output.append(raw)
    return output[0], output[1]


def _completion(reader: _Reader, row: dict[str, Any], spec: execution.RoleSpec,
                donor: execution.ProbeDonorEvidence | None = None) -> tuple[str, str, engine.ProcessPolicy, int]:
    """Reconstruct finite exit independently of stream or shared-state completeness."""
    owner, policy = reader.owner, reader.owner.policy
    probe = spec.role == "probe"
    creation = row["label"] + ("-probe" if probe else "-cli")
    created = _created(reader, creation, spec)
    cid = created["Id"]
    require(cid == row["probe_id" if probe else "cli_id"], "Observation process identity differs")
    label = creation if probe else row["label"] + "-cli-process"
    process = row["process"]
    same(process, reader.json(label + "-process.json"), "Original process copy differs")
    envelope = wire.max_probe_output_bytes(policy.wire_limits) if probe else policy.cli_stream_limit_bytes
    process_policy = engine.ProcessPolicy(policy.image_id,
        timeout_seconds=policy.probe_timeout_seconds if probe else policy.cli_timeout_seconds,
        stream_limit_bytes=envelope, frame_limit_bytes=envelope,
        transport_timeout_seconds=policy.transport_timeout_seconds)
    before = reader.inspect(label + ("-prestart" if probe else "-inspect-before"), cid)
    execution.validate_state(before, "created")
    if probe:
        reader.compare(label + "-prestart-comparison", created, before, spec, "created-to-prestart")
        assert donor is not None
        same(process["donor"], donor.record(), "Original probe donor differs")
        same(reader.json(label + "-intent.json"), {"protocol": execution.PROTOCOL, "role": asdict(spec),
            "expected": execution.digest(created), "runtime": owner.runtime, "policy": asdict(policy),
            "role_policy": execution.role_policy_identity(), "donor": donor.record(),
            "helper_stdout_envelope": envelope, "helper_sha256": wire.helper_sha256()}, "Original probe intent differs")
    else:
        engine.validate_sandbox(before, process_policy, expected_argv=list(spec.argv), runtime=owner.runtime)
        comparison = engine.identity_comparison(engine.immutable_inspection(created), engine.immutable_inspection(before),
            owner.runtime, phase="candidate-created-to-prestart", before_full_inspection=created, after_full_inspection=before)
        require(comparison["matches"] is True, "CLI prestart identity differs")
        same(comparison, process["prestart_comparison"], "CLI prestart record differs")
        declaration = reader.json(label + "-intent.json")
        same(declaration["expected_argv"], list(spec.argv), "CLI argv differs")
        same(declaration["expected_runtime"], owner.runtime, "CLI runtime differs")
        same(declaration["policy"], asdict(process_policy), "CLI policy differs")
        require(declaration["protocol"] == engine.PROTOCOL and declaration["container_id"] == cid
                and declaration["expected_full_inspection_sha256"] == execution.sha256(engine._comparison_bytes(created)),
                "CLI intent source process differs")
    body = reader.control(label + "-start", cid, "start", "POST", 204)
    start = process["start_response"]
    require(start["status"] == 204 and start["body_bytes"] == len(body) and start["body_sha256"] == execution.sha256(body)
            and start["framing_complete"] is True and start["eof_observed"] is True,
            "Original finite start event differs")
    require(reader.descriptor(start["request"]) == reader.raw(label + "-start-request.bin")
            and reader.descriptor(start["response"]) == reader.raw(label + "-start-response.bin"), "Start original bytes differ")
    waited = engine.strict_json_loads(reader.control(label + "-wait", cid, "wait?condition=not-running", "POST"))
    final = reader.inspect(label + ("-final" if probe else "-inspect-final"), cid)
    if probe:
        comparison = reader.compare(label + "-identity-comparison", created, final, spec, "created-to-exited", donor)
        same(process["identity_comparison"], comparison, "Probe final comparison differs")
    else:
        comparison = engine.startup_identity_comparison(engine.immutable_inspection(created), engine.immutable_inspection(final),
            owner.runtime, phase="candidate-created-to-exited", before_full_inspection=created, after_full_inspection=final)
        require(comparison["matches"] is True, "CLI final identity differs")
        same(comparison, process["startup_comparison"], "CLI final comparison differs")
    completion = engine.completion_evidence(waited, final, started=True, killed=False, identity_ok=True)
    same(process["completion"], completion, "Raw wait/inspection differs from completion record")
    require(process["protocol"] == (execution.PROTOCOL if probe else engine.PROTOCOL)
            and completion["natural"] is True, "Original natural finite completion unavailable")
    return cid, label, process_policy, int(completion["inspect_exit_code"])


def _process(reader: _Reader, row: dict[str, Any], spec: execution.RoleSpec,
             donor: execution.ProbeDonorEvidence | None = None) -> tuple[bytes, bytes, int]:
    cid, label, process_policy, exit_code = _completion(reader, row, spec, donor)
    process = row["process"]
    require(process["status"] == "completed", "Original complete stream observation unavailable")
    stdout, stderr = _streams(reader, label, cid, process, process_policy)
    if spec.role == "probe":
        require(exit_code == 0 and not stderr, "Trusted helper failed or emitted diagnostics")
    return stdout, stderr, exit_code


def _facts(observation: wire.WireObservation, limits: wire.WireLimits) -> semantics.ResponseFacts:
    extracted = head.extract_response_head(observation.received, limits)
    body: bytes | semantics.Missing = observation.response.body if (
        observation.sent_complete and observation.response.body_complete and observation.response.framing_complete
    ) else semantics.Missing(";".join(observation.limitations) or "complete response framing unavailable")
    before: semantics.ListenerFacts | semantics.Missing = semantics.Missing("no connected pre-request listener observation")
    after: semantics.ListenerFacts | semantics.Missing = semantics.Missing("no connected request interval")
    if observation.connected and observation.listeners_before_stage == "connected_pre_request":
        def listeners(value: wire.ListenerSnapshot) -> semantics.ListenerFacts:
            return semantics.ListenerFacts(tuple(address for _, address, _ in value.listeners),
                ";".join(value.limitations) or (None if value.complete else "listener coverage incomplete"))
        before, after = listeners(observation.listeners_before), listeners(observation.listeners_after)
    status = extracted.status if observation.sent_complete else semantics.Missing("complete intended request send unproven")
    headers = extracted.headers if observation.sent_complete else semantics.Missing("complete intended request send unproven")
    return semantics.ResponseFacts(status, headers, body, before, after)


def observe_execution(owner: execution.CandidateHttpExecution,
                      expected_checkpoint: execution.ControllerCheckpoint) -> HistoryObservation:
    """Verify exact originals without dispatch; return all steps, including unrun."""
    require(type(owner) is execution.CandidateHttpExecution
            and type(expected_checkpoint) is execution.ControllerCheckpoint,
            "Exact v4 owner and externally retained checkpoint required")
    require(owner.mode == "physical", "Fixture journal has no physical observation authority")
    reader = _Reader(owner, expected_checkpoint)
    result = owner.verified_execution()
    reader.validate_checkpoint()
    intent, terminal = reader.json("intent.json"), reader.json("terminal.json")
    rows = {reader.json(item["path"])["step_index"]: reader.json(item["path"]) for item in terminal["steps"]}
    probes = {row["step_index"]: row for row in result.observations}
    cli = {row["step_index"]: row for row in result.cli_observations}
    entered = set(terminal["entered_step_indices"])
    keeper_spec = _spec(reader, intent, None, "keeper")
    keeper: dict[str, Any] | None = None
    epochs: dict[int, tuple[execution.RoleSpec, dict[str, Any]]] = {}
    output = []
    for index, step in enumerate(owner.recipe.steps):
        label = "step-" + str(index).zfill(3)
        row = probes.get(index) if step.kind == "probe" else cli.get(index) if step.kind == "cli" else rows.get(index)
        authenticated = row is not None and (row.get("authenticated") is True if step.kind in ("probe", "cli") else index in rows)
        state = "authenticated" if authenticated else "unavailable" if index in entered else "unentered"
        facts = None
        stdout = stderr = None
        exit_code = None
        observed = None
        limits: tuple[str, ...] = ()
        provenance: tuple[tuple[str, str], ...] = (("definition_sha256", owner.recipe.definition_sha256), ("step_sha256", execution.digest(step.record())))
        if not authenticated:
            limits = (str(row.get("observation_error", "original observation unavailable")) if row is not None
                      else "entered without authenticated result" if index in entered else "step not entered",)
            if step.kind == "cli" and row is not None and type(row.get("process")) is dict:
                try:
                    _, _, _, exit_code = _completion(reader, row, _spec(reader, intent, index, "cli"))
                except (chain.ChainUnknown, execution.ExecutionUnknown):
                    raise
                except (ValueError, OSError, KeyError, TypeError) as error:
                    limits += ("independent finite exit unavailable: " + type(error).__name__,)
                else:
                    # Source/argv-bound natural exit does not establish DB lineage
                    # or complete streams. Those facts remain unavailable.
                    proof = execution.digest({"protocol": PROTOCOL, "source": owner.binding.source_sha256,
                        "step": step.record(), "process": row["process"]})
                    provenance += (("finite_exit_proof_sha256", proof),)
                    limits += ("only independent finite exit is authenticated; shared-state semantics remain unavailable",)
        else:
            assert row is not None
            if keeper is None:
                keeper = _long_role(reader, "keeper", keeper_spec)
            reader.boundary(label + "-keeper-before", keeper, keeper_spec)
            if step.kind == "start":
                spec = _spec(reader, intent, index, "server")
                server = _long_role(reader, label, spec)
                require(server["Id"] == row["server_id"] and step.epoch not in epochs, "Server epoch identity differs")
                epochs[step.epoch] = spec, server
            elif step.kind == "stop":
                require(step.epoch in epochs, "Stop has no authenticated server epoch")
                spec, server = epochs[step.epoch]
                reader.boundary(label + "-stop-before", server, spec)
                body = reader.control(label + "-stop", server["Id"], "stop?t=" + str(owner.policy.stop_timeout_seconds), "POST", 204)
                final = reader.inspect(label + "-stop-final", server["Id"])
                execution.validate_state(final, "exited")
                comparison = reader.compare(label + "-stop-comparison", server, final, spec, "running-to-exited")
                same(row["stop"], {"controller_action": "explicit-stop", "status": 204,
                    "response_body_sha256": execution.sha256(body), "container_id": server["Id"],
                    "exit_code": final["State"].get("ExitCode"), "natural_exit_zero_required": False,
                    "comparison": comparison, "final_inspection_sha256": execution.digest(final)}, "Explicit stop evidence differs")
                require(row["removed"] is True and not reader.command(label + "-retire-absence", ["docker", "container", "ls",
                    "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"]).strip(), "Previous server retirement unproven")
            elif step.kind == "probe":
                require(step.epoch in epochs, "Probe has no authenticated server epoch")
                spec, server = epochs[step.epoch]
                current = reader.inspect(label + "-server-before", server["Id"])
                donor = execution.bind_probe_donor(baseline=server, current=current, server_spec=spec,
                    registration=owner.registration, runtime=owner.runtime, epoch=step.epoch)
                same(row["donor"], donor.record(), "Raw probe donor differs")
                same(row["continuity_before"], reader.compare(label + "-server-before-continuity", server, current,
                    spec, "running-to-running"), "Probe pre-request continuity differs")
                stdout, stderr, exit_code = _process(reader, row, _spec(reader, intent, index, "probe", server["Id"]), donor)
                same(row["continuity_after"], reader.boundary(label + "-server-after", server, spec), "Probe post-request continuity differs")
                same(row["keeper_continuity"], reader.boundary(label + "-probe-keeper-after", keeper, keeper_spec), "Probe keeper continuity differs")
                request = engine.strict_json_loads(step.request_json)
                require(reader.raw(label + "-probe-input.json") == wire.build_probe_input(request, owner.recipe.port, owner.policy.wire_limits),
                        "Original probe input differs from registered request")
                observed = wire.decode_probe_output(stdout, request, owner.recipe.port, owner.policy.wire_limits)
                facts = _facts(observed, owner.policy.wire_limits)
                limits = tuple(observed.limitations)
            else:
                spec = _spec(reader, intent, index, "cli")
                stdout, stderr, exit_code = _process(reader, row, spec)
                same(row["keeper_continuity"], reader.boundary(label + "-cli-keeper-after", keeper, keeper_spec), "CLI keeper continuity differs")
                for phase in ("before", "after"):
                    volume = engine.strict_json_loads(reader.command(label + "-cli-volume-" + phase,
                        ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]]))
                    require(owner._volume_valid(volume, intent, reader.json("volume-baseline.json")), "CLI shared state volume changed")
            # A retained observation may survive a later boundary/retirement failure.
            # Its own above proof remains useful; absent final step is not a pass.
            if index in rows:
                reader.boundary(label + "-keeper-after", keeper, keeper_spec)
            provenance += (("row_sha256", execution.digest({k: v for k, v in row.items() if k != "wire"})),)
        output.append(StepObservation(step.step_id, index, step.kind, state, facts, stdout, stderr, exit_code,
                                      observed, limits, provenance))
    reader.validate_checkpoint()
    verified_after = owner.verified_execution()
    reader.validate_checkpoint()
    require(verified_after == result, "Original verification changed during observation")
    return HistoryObservation(result.execution_id, admission.binding_sha256(owner.binding, gate=owner.actual_registration.gate), result.terminal_sha256,
        execution.digest(asdict(expected_checkpoint)), tuple(output), result.missing_step_ids,
        result.cleanup_verified, result.infrastructure)
