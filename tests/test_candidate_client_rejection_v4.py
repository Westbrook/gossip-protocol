"""Offline independent retained-evidence qualification controls; no Engine access."""
from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
from dataclasses import dataclass
import json
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_candidate_clients_docker_v4 as qualification
from test_candidate_client_process_v4 import FakeSocket, fixture_inspection, runtime_fixture

ABSENT_ARGV = ["/workspace/absent-control-executable"]
MESSAGE = "failed to create task: no such file or directory"


def http_response(body, *, status=400, framing="length"):
    first = f"HTTP/1.1 {status} Result\r\n".encode("ascii")
    if framing == "length":
        return first + f"Content-Length: {len(body)}\r\n".encode("ascii") + b"\r\n" + body
    if framing == "chunked":
        return first + b"Transfer-Encoding: chunked\r\n\r\n" + (
            f"{len(body):x}\r\n".encode("ascii") + body + b"\r\n" if body else b""
        ) + b"0\r\n\r\n"
    return first + b"Connection: close\r\n\r\n" + body


def proof_fixture(*, before=None, after=None, cli_after=None, body=None, status=400, framing="length"):
    """Authored inert dictionaries/bytes, never copied from a physical run."""
    runtime = runtime_fixture()
    if before is None:
        before = fixture_inspection()
        before["Path"] = ABSENT_ARGV[0]
        before["Args"] = []
        before["Config"].update(Entrypoint=ABSENT_ARGV[:], Cmd=None)
        before["Config"]["Labels"]["gossip.step"] = "raw-startup"
    before = deepcopy(before)
    if after is None:
        after = deepcopy(before)
        after["HostConfig"]["OomKillDisable"] = None
        after["State"].update(ExitCode=127, Error=MESSAGE)
    after = deepcopy(after)
    cli_after = deepcopy(after if cli_after is None else cli_after)
    if body is None:
        body = qualification.encoded({"message": MESSAGE})
    identity = before["Id"]
    artifacts, checkpoints = {}, {}
    def put(name, raw):
        artifacts[name] = raw
        checkpoints[name] = {"index": len(checkpoints), "sha256": qualification.sha256(raw)}
        return qualification.raw_descriptor(name, raw)
    def owned(name, value):
        return put(name, qualification.encoded(value))
    def command(label, value):
        descriptor = put(label + "-stdout.bin", qualification.encoded(value))
        return owned(label + ".json", {"exit_code": 0, "timed_out": False, "capture_complete": True,
            "stdout": {**descriptor, "truncated": False}, "stderr": {"truncated": False}})
    def engine(label, value):
        put(label + "-request.bin", (
            "GET /v1.47/containers/" + identity + "/json HTTP/1.1\r\nHost: docker\r\n"
            "Connection: close\r\nContent-Length: 0\r\n\r\n").encode())
        put(label + "-response.bin", http_response(qualification.encoded(value), status=200))
    owned("config.json", {"runtime": runtime})
    owned("raw-container-create.json", {"argv": ["docker", "create"]})
    command("raw-container-created", before)
    owned("raw-startup-intent.json", {
        "protocol": qualification.transport.PROTOCOL, "container_id": identity,
        "expected_runtime": runtime, "expected_argv": ABSENT_ARGV[:],
        "expected_argv_sha256": qualification.sha256(qualification.encoded(ABSENT_ARGV)),
        "expected_full_inspection_sha256": qualification.sha256(qualification.encoded(before)),
        "start_response_policy": qualification.transport.start_response_policy(),
        "start_response_policy_sha256": qualification.transport.start_response_policy_sha256()})
    engine("raw-startup-inspect-before", before)
    precompare = qualification.transport.identity_comparison(
        qualification.transport.immutable_inspection(before),
        qualification.transport.immutable_inspection(before), runtime,
        phase="candidate-created-to-prestart", before_full_inspection=before, after_full_inspection=before)
    owned("raw-startup-prestart-comparison.json", precompare)
    put("raw-startup-attach-request.bin", (
        "POST /v1.47/containers/" + identity + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0 HTTP/1.1\r\n"
        "Host: docker\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n").encode())
    target = "/v1.47/containers/" + identity + "/start"
    request = put("raw-startup-start-request.bin", ("POST " + target + " HTTP/1.1\r\nHost: docker\r\n"
        "Connection: close\r\nContent-Length: 0\r\n\r\n").encode())
    response = put("raw-startup-start-response.bin", http_response(body, status=status, framing=framing))
    response_proof = {"protocol": qualification.transport.START_RESPONSE_PROTOCOL, "method": "POST",
        "request_path": target, "container_id": identity,
        "endpoint_sha256": qualification.sha256(qualification.encoded(runtime["endpoint"])),
        "runtime_sha256": qualification.sha256(qualification.encoded(runtime)),
        "request": request, "response": response, "status": status, "body_bytes": len(body),
        "body_sha256": qualification.sha256(body), "framing_complete": True,
        "eof_observed": True, "durably_retained": True}
    receipt = owned("raw-startup-start-response-completion.json", response_proof)
    put("raw-startup-kill-request.bin", ("POST /v1.47/containers/" + identity +
        "/kill?signal=SIGKILL HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode())
    engine("raw-startup-inspect-after-kill", after)
    owned("raw-startup-cleanup-state.json", after)
    stdout = put("raw-startup-stdout.bin", b"")
    stderr = put("raw-startup-stderr.bin", b"")
    process = {"protocol": qualification.transport.PROTOCOL, "status": "start_error", "exit_code": None,
        "completion": {"started": False, "natural": False, "killed": True},
        "startup_comparison": None, "prestart_comparison": precompare,
        "start_response": response_proof, "start_response_receipt": receipt,
        "stdout": stdout, "stderr": stderr,
        "evidence": [name for name in artifacts if name.startswith("raw-startup-")]}
    owned("raw-startup-process.json", process)
    owned("raw-startup-result.json", process)
    command("raw-container-after-start", cli_after)
    command("raw-container-cleanup-inspect", cli_after)
    return {"artifacts": artifacts, "checkpoints": checkpoints, "runtime": runtime,
        "expected_argv": ABSENT_ARGV[:], "container_id": identity,
        "labels": deepcopy(before["Config"]["Labels"])}, before, after


def replace_artifact(bundle, name, raw):
    """Keep outer checkpoint authentic to test the inner semantic boundary."""
    bundle["artifacts"][name] = raw
    bundle["checkpoints"][name]["sha256"] = qualification.sha256(raw)


def replace_owned(bundle, name, value):
    replace_artifact(bundle, name, qualification.encoded(value))


def change_process(bundle, change):
    value = json.loads(bundle["artifacts"]["raw-startup-process.json"])
    change(value)
    for name in ("raw-startup-process.json", "raw-startup-result.json"):
        replace_owned(bundle, name, value)


def change_receipt(bundle, change):
    value = json.loads(bundle["artifacts"]["raw-startup-start-response-completion.json"])
    change(value)
    raw = qualification.encoded(value)
    replace_artifact(bundle, "raw-startup-start-response-completion.json", raw)
    def apply(process):
        process["start_response"] = value
        process["start_response_receipt"] = qualification.raw_descriptor(
            "raw-startup-start-response-completion.json", raw)
    change_process(bundle, apply)


def retained_qualification_fixture(root, *, mutation=None):
    """Materialize only authored evidence and call the actual final qualifier.

    The controller supplies authenticated retained files and pure ownership
    checks. Its methods cannot create containers, start processes, or use Git.
    """
    bundle, before, _ = proof_fixture()
    artifacts, checkpoints = bundle["artifacts"], bundle["checkpoints"]
    labels = bundle["labels"]
    identity, runtime = bundle["container_id"], bundle["runtime"]
    container_name = before["Name"].removeprefix("/")
    volume = next(row["Name"] for row in before["Mounts"] if row["Destination"] == "/tmp")
    intent = {"execution_id": labels["gossip.execution"], "volume": volume}
    step = {"step_id": labels["gossip.step"], "argv": ABSENT_ARGV[:]}
    prefix = ["docker", "--host", "unix://" + runtime["endpoint"]["socket_path"]]
    def put(name, raw):
        artifacts[name] = raw
        checkpoints[name] = {"index": len(checkpoints), "sha256": qualification.sha256(raw)}
        return qualification.raw_descriptor(name, raw)
    def command(label, argv, stdout=b"", stderr=b""):
        stdout_descriptor = put(label + "-stdout.bin", stdout)
        stderr_descriptor = put(label + "-stderr.bin", stderr)
        put(label + ".json", qualification.encoded({"argv": prefix + argv,
            "exit_code": 0, "timed_out": False, "capture_complete": True,
            "stdout": {**stdout_descriptor, "truncated": False},
            "stderr": {**stderr_descriptor, "truncated": False}}))
    command("raw-container-remove", ["rm", "--force", identity], identity.encode() + b"\n")
    command("raw-container-absent", ["container", "ls", "--all", "--quiet", "--filter", "name=^/" + container_name + "$"])
    volume_view = {"Name": volume, "Driver": "local", "Options": deepcopy(qualification.execution.VOLUME_OPTIONS),
        "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": qualification.execution.SNAPSHOT_PROTOCOL}}
    command("raw-volume-cleanup-inspect", ["volume", "inspect", "--format", "{{json .}}", volume],
        qualification.encoded(volume_view))
    command("raw-volume-remove", ["volume", "rm", volume], volume.encode() + b"\n")
    command("raw-volume-after", ["volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"])
    process = json.loads(artifacts["raw-startup-process.json"])
    terminal = {"cleanup": {"container": True, "volume": True}, "cleanup_errors": [],
        "record": process, "created_container_id": identity,
        "successfully_started_candidate_processes": 0, "naturally_completed_candidate_processes": 0}
    cutoff = checkpoints["raw-container-created.json"]["index"]
    before_checkpoint = {"files": [[name, row["sha256"]] for name, row in checkpoints.items()
        if row["index"] <= cutoff]}
    state = {"bundle": bundle, "terminal": terminal, "before_checkpoint": before_checkpoint}
    if mutation is not None:
        mutation(state)
    root.mkdir()
    journal = root / "execution"
    journal.mkdir()
    for name, raw in artifacts.items():
        (journal / name).write_bytes(raw)
    retained = {}
    for index, (name, row) in enumerate(sorted(checkpoints.items(), key=lambda item: item[1]["index"])):
        retained[name] = row["sha256"]
        (root / f"external-checkpoint-{index:04d}.json").write_bytes(
            qualification.encoded({"files": sorted(retained.items())}))
    (root / "raw-transport-terminal.json").write_bytes(qualification.encoded(terminal))
    if state["before_checkpoint"] is not None:
        (root / "external-before-start-checkpoint.json").write_bytes(
            qualification.encoded(state["before_checkpoint"]))
    class RetainedController:
        def __init__(self):
            self.root = journal
            self.binding = SimpleNamespace(source_sha256=labels["gossip.source"])
            self.config = {"fixture_sha256": labels["gossip.fixture"]}
            self.authenticated = {name: row["sha256"] for name, row in checkpoints.items()}
            self.retained_names = []
        def checkpoint(self):
            return qualification.execution.ControllerCheckpoint(tuple(sorted(self.authenticated.items())))
        def _retain(self, name, raw):
            with (self.root / name).open("xb") as stream:
                stream.write(raw)
            self.authenticated[name] = qualification.sha256(raw)
            self.retained_names.append(name)
        _raw = qualification.execution.CandidateClientExecution._raw
        _volume_valid = qualification.execution.CandidateClientExecution._volume_valid
        _clean = staticmethod(qualification.execution.CandidateClientExecution._clean)
    controller = RetainedController()
    kwargs = {"runtime": runtime, "step": step, "intent": intent, "name": container_name,
        "volume": volume, "container_id": identity}
    return controller, process, kwargs


def composed_raw_control_fixture(test, root, *, lose_start_eof=False):
    """Real controller/transport/qualification composition over fake IO only."""
    transport, execution = qualification.transport, qualification.execution
    runtime = runtime_fixture()
    image = {"Id": runtime["image_id"], "Os": "linux"}
    runtime["image_inspect_sha256"] = qualification.sha256(qualification.encoded(image))
    version = {"MinAPIVersion": "1.24", "ApiVersion": "1.47", "Os": runtime["os"],
        "GitCommit": runtime["engine_git_commit"], "Version": runtime["engine_version"],
        "Arch": runtime["architecture"], "KernelVersion": runtime["kernel_version"]}
    info = {"ID": runtime["daemon_id"], "OSType": runtime["os"], "CgroupVersion": runtime["cgroup_version"],
        "CgroupDriver": runtime["cgroup_driver"], "OomKillDisable": runtime["oom_kill_disable_supported"]}
    test.runtime = runtime
    test.endpoint = transport.EngineEndpoint(**runtime["endpoint"])
    test.policy = execution.ClientPolicy(runtime["image_id"])
    test.requirements_sha256 = qualification.cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
    row = next(item for item in qualification.control_definitions()
        if item["control_id"] == "startup-missing-executable")
    identity = "b" * 64
    state = {"container": None, "volume": None, "container_present": False, "volume_present": False,
        "starts": 0, "kills": 0, "cli": [], "http": [], "sockets": []}
    gate = threading.Event()
    def current_inspection():
        value = deepcopy(state["container"])
        test.assertIsNotNone(value, "Inspection requires preceding fake CLI creation")
        if state["starts"]:
            value["HostConfig"]["OomKillDisable"] = None
            value["State"].update(ExitCode=127, Error=MESSAGE)
        return value
    class RoutedSocket(FakeSocket):
        def __init__(self):
            super().__init__(b"", chunks=97)
            self.target = None
            state["sockets"].append(self)
        def sendall(self, raw):
            super().sendall(raw)
            method, target, protocol = raw.split(b"\r\n", 1)[0].decode("ascii").split(" ")
            test.assertEqual(protocol, "HTTP/1.1")
            self.target = target
            state["http"].append((method, target))
            if (method, target) == ("GET", "/v1.47/version"):
                response = http_response(qualification.encoded(version), status=200)
            elif (method, target) == ("GET", "/v1.47/info"):
                response = http_response(qualification.encoded(info), status=200)
            elif (method, target) == ("GET", "/v1.47/images/" + runtime["image_id"] + "/json"):
                response = http_response(qualification.encoded(image), status=200)
            elif (method, target) == ("GET", "/v1.47/containers/" + identity + "/json"):
                response = http_response(qualification.encoded(current_inspection()), status=200)
            elif (method, target) == ("POST", "/v1.47/containers/" + identity + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0"):
                response = (b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n"
                    b"Content-Type: application/vnd.docker.multiplexed-stream\r\n\r\n")
                self.gate = gate
            elif (method, target) == ("POST", "/v1.47/containers/" + identity + "/start"):
                state["starts"] += 1
                test.assertEqual(state["starts"], 1, "One attempt only; no retry")
                response = http_response(qualification.encoded({"message": MESSAGE}), status=400)
                if lose_start_eof:
                    self.eof_error = ConnectionResetError("Authored fake socket reset before witnessed EOF")
            elif (method, target) == ("POST", "/v1.47/containers/" + identity + "/kill?signal=SIGKILL"):
                state["kills"] += 1
                response = http_response(b'{"message":"container is not running"}', status=409)
            else:
                raise AssertionError("Unregistered fake Engine request: " + method + " " + target)
            self.response = bytearray(response)
            self.header_remaining = response.find(b"\r\n\r\n") + 4
        def close(self):
            if self.target == "/v1.47/containers/" + identity + "/start":
                gate.set()
            super().close()
        def shutdown(self, how):
            if self.gate is gate:
                gate.set()
            super().shutdown(how)
    def options(argv, flag):
        return [argv[index + 1] for index, value in enumerate(argv[:-1]) if value == flag]
    def fake_command(controller, label, argv, timeout=None):
        test.assertEqual(argv[0], "docker")
        state["cli"].append(label)
        if label == "raw-volume-before":
            test.assertFalse(state["volume_present"])
            raw = b""
        elif label == "raw-volume-create":
            labels = dict(item.split("=", 1) for item in options(argv, "--label"))
            values = dict(item.split("=", 1) for item in options(argv, "--opt"))
            state["volume"] = {"Name": argv[-1], "Driver": "local", "Labels": labels, "Options": values}
            state["volume_present"] = True
            raw = (argv[-1] + "\n").encode()
        elif label in ("raw-volume-created", "raw-volume-cleanup-inspect"):
            test.assertTrue(state["volume_present"])
            test.assertEqual(argv[-1], state["volume"]["Name"])
            raw = qualification.encoded(state["volume"])
        elif label == "raw-container-before":
            test.assertFalse(state["container_present"])
            raw = b""
        elif label == "raw-container-create":
            executable = argv[argv.index("--entrypoint") + 1]
            test.assertEqual(executable, ABSENT_ARGV[0])
            test.assertEqual(argv[argv.index("--entrypoint") + 2:], [runtime["image_id"]])
            value = fixture_inspection()
            value.update(Id=identity, Name="/" + argv[argv.index("--name") + 1], Path=executable, Args=[])
            value["Config"].update(Entrypoint=[executable], Cmd=None,
                Labels=dict(item.split("=", 1) for item in options(argv, "--label")))
            value["Mounts"][0]["Source"] = str(root / "workspace")
            value["Mounts"][1]["Source"] = str(root / "inputs")
            value["Mounts"][2]["Name"] = state["volume"]["Name"]
            state["container"] = value
            state["container_present"] = True
            raw = (identity + "\n").encode()
        elif label in ("raw-container-created", "raw-container-after-start", "raw-container-cleanup-inspect"):
            test.assertTrue(state["container_present"])
            test.assertIn(argv[-1], (identity, state["container"]["Name"].removeprefix("/")))
            raw = qualification.encoded(current_inspection())
        elif label == "raw-container-remove":
            test.assertEqual(argv, ["docker", "rm", "--force", identity])
            state["container_present"] = False
            raw = (identity + "\n").encode()
        elif label == "raw-container-absent":
            test.assertFalse(state["container_present"])
            raw = b""
        elif label == "raw-volume-remove":
            test.assertEqual(argv, ["docker", "volume", "rm", state["volume"]["Name"]])
            state["volume_present"] = False
            raw = (state["volume"]["Name"] + "\n").encode()
        elif label == "raw-volume-after":
            test.assertFalse(state["volume_present"])
            raw = b""
        else:
            raise AssertionError("Unregistered fake CLI command: " + label)
        record = {"argv": ["docker", "--host", "unix://" + runtime["endpoint"]["socket_path"], *argv[1:]],
            "exit_code": 0, "timed_out": False, "capture_complete": True}
        for channel, data in (("stdout", raw), ("stderr", b"")):
            path = label + "-" + channel + ".bin"
            controller._retain(path, data)
            record[channel] = {**qualification.raw_descriptor(path, data),
                "observed_bytes": len(data), "truncated": False}
        controller._retain(label + ".json", qualification.encoded(record))
        return record
    def fake_store(path, files):
        test.assertEqual(files, row["files"])
        return SimpleNamespace(path=Path(path).resolve(), head=lambda: "a" * 40)
    result, error = None, None
    with ExitStack() as stack:
        stack.enter_context(patch.object(qualification, "make_store", side_effect=fake_store))
        stack.enter_context(patch.object(execution, "capture_git_source", return_value=("b" * 40, row["files"])))
        stack.enter_context(patch.object(execution.CandidateClientExecution, "_command", fake_command))
        stack.enter_context(patch.object(transport.EngineEndpoint, "validate"))
        stack.enter_context(patch.object(transport.socket, "socket", side_effect=lambda *_args, **_kwargs: RoutedSocket()))
        stack.enter_context(patch.object(transport.subprocess, "Popen", side_effect=AssertionError("No subprocess permitted")))
        try:
            result = qualification.startup_transport_control(test, row, root)
        except (AssertionError, qualification.QualificationError, execution.ExecutionError) as caught:
            error = caught
        finally:
            gate.set()
    return {"result": result, "error": error, "state": state}


@dataclass
class RawFixtureRegistration:
    binding: dict
    commit_oid: str
    tree_oid: str
    repetition_id: str


def exercise_raw_decoder_boundaries(test, root, *, malformed_label=None, missing_proof=False):
    """Use real raw helper/decoders with an inert fake controller and transport.

    The separate full-bundle tests exercise proof authentication. This fixture
    isolates decoder-before-use and exactly-once dispatch boundaries, and cannot
    open a socket, invoke Docker/Git, or execute any staged source.
    """
    bundle, before, after = proof_fixture()
    process_record = json.loads(bundle["artifacts"]["raw-startup-process.json"])
    if missing_proof:
        process_record["start_response"] = None
        change_process(bundle, lambda value: value.update(start_response=None))
    test.runtime = bundle["runtime"]
    test.endpoint = qualification.transport.EngineEndpoint(**test.runtime["endpoint"])
    test.policy = qualification.execution.ClientPolicy(test.runtime["image_id"])
    test.requirements_sha256 = "d" * 64
    controllers = []
    events = []
    malformed = b'{"Id":"one","Id":"two"}'
    class Controller:
        def __init__(self, journal, *args, **kwargs):
            self.root = journal
            self.root.mkdir()
            self.volume_validations = []
            self.created_validations = []
            self.commands = []
            controllers.append(self)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def _retain(self, name, raw):
            (self.root / name).write_bytes(raw)
            events.append("retain:" + name)
        @staticmethod
        def _clean(record):
            return record["exit_code"] == 0 and record["capture_complete"] is True
        @staticmethod
        def _raw(record, kind="stdout"):
            return record.get("fixture_raw", b"")
        def _command(self, label, argv):
            self.commands.append(label)
            if label == malformed_label:
                raw = malformed
            elif label in ("raw-volume-created", "raw-volume-cleanup-inspect"):
                raw = b'{"Name":"fixture-volume","Options":{"size":"16m"}}'
            elif label in ("raw-container-created", "raw-container-after-start"):
                raw = qualification.encoded(before if label.endswith("created") else after)
            elif label == "raw-container-create":
                raw = before["Id"].encode()
            elif label == "raw-volume-create":
                raw = argv[-1].encode()
            else:
                raw = b""
            return {"fixture_raw": raw, "exit_code": 0, "capture_complete": True}
        _checked = _command
        def _create_argv(self, *args):
            return ["fixture-create-never-dispatched"]
        def _volume_valid(self, value, intent):
            self.volume_validations.append(value)
            return True
        def _validate_created(self, value, **kwargs):
            self.created_validations.append(value)
            qualification.transport.validate_sandbox(value, test.policy.process_policy(),
                expected_argv=kwargs["step"]["argv"], runtime=test.runtime)
        def checkpoint(self):
            return qualification.execution.ControllerCheckpoint(())
        def _remove_container(self, *args):
            events.append("remove-container")
            return True
    def fake_run(*args, **kwargs):
        events.append("start")
        test.assertEqual(kwargs["expected_argv"], ABSENT_ARGV)
        test.assertIs(kwargs["expected"]["Config"]["Cmd"], None)
        return deepcopy(process_record)
    def qualify_after_cleanup(testcase, controller, path, record, **kwargs):
        events.append("qualify")
        terminal = json.loads((path / "raw-transport-terminal.json").read_bytes())
        testcase.assertEqual(terminal["cleanup"], {"container": True, "volume": True})
        testcase.assertEqual(terminal["cleanup_errors"], [])
        testcase.assertIn("remove-container", events)
        # The raw helper cannot turn the generic label into rejection proof.
        return qualification.validate_start_rejection(bundle)
    row = next(item for item in qualification.control_definitions()
        if item["control_id"] == "startup-missing-executable")
    store = SimpleNamespace(head=lambda: "a" * 40)
    with ExitStack() as stack:
        stack.enter_context(patch.object(qualification, "make_store", return_value=store))
        stack.enter_context(patch.object(qualification.execution, "capture_git_source", return_value=("b" * 40, row["files"])))
        stack.enter_context(patch.object(qualification.execution, "binding_for", return_value={"source_sha256": "c" * 64}))
        stack.enter_context(patch.object(qualification.execution, "ClientRegistration", RawFixtureRegistration))
        stack.enter_context(patch.object(qualification.execution, "CandidateClientExecution", Controller))
        dispatch = stack.enter_context(patch.object(qualification.transport, "run_process", side_effect=fake_run))
        stack.enter_context(patch.object(qualification, "assert_startup_policy_binding", return_value=test.runtime))
        stack.enter_context(patch.object(qualification, "assert_process_startup_record"))
        stack.enter_context(patch.object(qualification, "qualify_rejected_start", side_effect=qualify_after_cleanup))
        stack.enter_context(patch.object(qualification.transport.socket, "socket", side_effect=AssertionError("No socket permitted")))
        stack.enter_context(patch.object(qualification.transport.subprocess, "Popen", side_effect=AssertionError("No subprocess permitted")))
        result, error = None, None
        try:
            result = qualification.startup_transport_control(test, row, root)
        except (qualification.QualificationError, AssertionError) as caught:
            error = caught
        return {"result": result, "error": error, "dispatch_count": dispatch.call_count,
            "controller": controllers[0], "events": events}


class CandidateClientRejectionProofV4Tests(unittest.TestCase):
    def test_independent_http_decodes_all_three_framings_with_witnessed_eof(self):
        body = b'{"message":"missing executable"}\n'
        for framing in ("length", "chunked", "close"):
            with self.subTest(framing=framing):
                parsed = qualification.parse_retained_http(
                    http_response(body, framing=framing), eof_observed=True)
                self.assertEqual(parsed["status"], 400)
                self.assertEqual(parsed["body"], body)
                self.assertIs(type(parsed["headers"]), dict)
        parsed = qualification.parse_retained_http(
            b"HTTP/1.1 204 No Content\r\n\r\n", eof_observed=True)
        self.assertEqual((parsed["status"], parsed["body"]), (204, b""))

    def test_complete_disk_bytes_never_substitute_for_witnessed_socket_eof(self):
        for framing in ("length", "chunked", "close"):
            with self.subTest(framing=framing), self.assertRaises(qualification.QualificationError):
                qualification.parse_retained_http(http_response(b"{}", framing=framing), eof_observed=False)
        for eof in (None, 1, "true"):
            with self.subTest(eof=eof), self.assertRaises(qualification.QualificationError):
                qualification.parse_retained_http(http_response(b"{}"), eof_observed=eof)

    def test_http_rejects_truncation_extra_response_and_ambiguous_length(self):
        cases = {
            "truncated-status": b"HTTP/1.1 40",
            "truncated-header": b"HTTP/1.1 400 Bad\r\nContent-Length: 2\r\n",
            "truncated-body": b"HTTP/1.1 400 Bad\r\nContent-Length: 3\r\n\r\n{}",
            "trailing-byte": http_response(b"{}") + b"x",
            "second-response": http_response(b"{}") + http_response(b"{}"),
            "negative-length": b"HTTP/1.1 400 Bad\r\nContent-Length: -1\r\n\r\n",
            "signed-length": b"HTTP/1.1 400 Bad\r\nContent-Length: +2\r\n\r\n{}",
            "decimal-length": b"HTTP/1.1 400 Bad\r\nContent-Length: 2.0\r\n\r\n{}",
            "length-and-chunks": b"HTTP/1.1 400 Bad\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}",
            "unsupported-transfer": b"HTTP/1.1 400 Bad\r\nTransfer-Encoding: gzip\r\n\r\n{}",
            "no-content-body": b"HTTP/1.1 204 No Content\r\nContent-Length: 2\r\n\r\n{}",
        }
        for name, raw in cases.items():
            with self.subTest(name=name), self.assertRaises(qualification.QualificationError):
                qualification.parse_retained_http(raw, eof_observed=True)

    def test_http_rejects_duplicate_folded_control_and_malformed_headers(self):
        cases = {
            "duplicate": b"Content-Length: 2\r\ncontent-length: 2",
            "duplicate-unrelated": b"Content-Length: 2\r\nX-Trace: a\r\nx-trace: b",
            "folded": b"Content-Length: 2\r\n X-Trace: value",
            "space-in-name": b"Content-Length: 2\r\nBad Name: value",
            "missing-colon": b"Content-Length: 2\r\nBadHeader",
            "control-value": b"Content-Length: 2\r\nX-Trace: a\x00b",
            "delete-control": b"Content-Length: 2\r\nX-Trace: a\x7fb",
            "nbsp-length": b"Content-Length: \xa02\xa0",
            "next-line-length": b"Content-Length: \x852\x85",
            "nbsp-transfer": b"Transfer-Encoding: \xa0chunked\xa0",
            "next-line-transfer": b"Transfer-Encoding: \x85chunked\x85",
            "bare-lf": b"Content-Length: 2\nX-Trace: value",
        }
        for name, headers in cases.items():
            body = b"2\r\n{}\r\n0\r\n\r\n" if name.endswith("transfer") else b"{}"
            raw = b"HTTP/1.1 400 Bad\r\n" + headers + b"\r\n\r\n" + body
            with self.subTest(name=name), self.assertRaises(qualification.QualificationError):
                qualification.parse_retained_http(raw, eof_observed=True)
        for status_line in (b"HTTP/2 400 Bad", b"HTTP/1.1 Bad", b"HTTP/1.1 400 Bad\x00"):
            with self.subTest(status_line=status_line), self.assertRaises(qualification.QualificationError):
                qualification.parse_retained_http(status_line + b"\r\nContent-Length: 2\r\n\r\n{}", eof_observed=True)

    def test_http_rejects_incomplete_or_noncanonical_chunk_framing(self):
        prefix = b"HTTP/1.1 400 Bad\r\nTransfer-Encoding: chunked\r\n\r\n"
        cases = {
            "missing-zero": b"2\r\n{}\r\n",
            "truncated-data": b"3\r\n{}",
            "bad-data-terminator": b"2\r\n{}XX0\r\n\r\n",
            "bad-length": b"xx\r\n{}\r\n0\r\n\r\n",
            "negative-length": b"-1\r\n\r\n0\r\n\r\n",
            "unsupported-extension": b"2;extension=x\r\n{}\r\n0\r\n\r\n",
            "trailer": b"2\r\n{}\r\n0\r\nX-Trailer: value\r\n\r\n",
            "truncated-zero": b"2\r\n{}\r\n0\r\n",
            "trailing-after-zero": b"2\r\n{}\r\n0\r\n\r\nx",
        }
        for name, suffix in cases.items():
            with self.subTest(name=name), self.assertRaises(qualification.QualificationError):
                qualification.parse_retained_http(prefix + suffix, eof_observed=True)

    def test_http_enforces_header_body_chunk_and_total_wire_bounds(self):
        cases = {
            "header-bytes": b"HTTP/1.1 400 Bad\r\nX:" + b"x" * 32768 + b"\r\n\r\n",
            "header-count": b"HTTP/1.1 400 Bad\r\n" + b"".join(
                f"X-{index}: v\r\n".encode("ascii") for index in range(128)) + b"\r\n",
            "declared-body": b"HTTP/1.1 400 Bad\r\nContent-Length: 1048577\r\n\r\n" + b"x" * 1048577,
            "close-body": http_response(b"x" * 1048577, framing="close"),
            "declared-chunk": b"HTTP/1.1 400 Bad\r\nTransfer-Encoding: chunked\r\n\r\n100001\r\n" + b"x" * 1048577 + b"\r\n0\r\n\r\n",
            "chunk-count": b"HTTP/1.1 400 Bad\r\nTransfer-Encoding: chunked\r\n\r\n" + b"1\r\nx\r\n" * 65537 + b"0\r\n\r\n",
            "raw-wire": b"x" * 2392065,
        }
        for name, raw in cases.items():
            expected = "HTTP wire bound/type" if name == "raw-wire" else ".*"
            with self.subTest(name=name), self.assertRaisesRegex(qualification.QualificationError, expected):
                qualification.parse_retained_http(raw, eof_observed=True)

    def test_strict_raw_decoder_preserves_types_unknown_fields_and_array_order(self):
        expected = {"Config": {"Cmd": None, "Entrypoint": ["/workspace/absent-control-executable"]},
            "Mounts": [{"Destination": "/tmp", "unknown": {"ordered": [2, 1]}}],
            "future": [False, None, 0, ""]}
        raw = json.dumps(expected, ensure_ascii=False).encode("utf-8")
        decoded = qualification.strict_inspection_json(raw)
        self.assertEqual(decoded, expected)
        self.assertIs(decoded["Config"]["Cmd"], None)
        self.assertIs(decoded["future"][0], False)
        self.assertIs(type(decoded["future"][2]), int)

    def test_strict_raw_decoder_rejects_duplicate_keys_at_every_relevant_depth(self):
        cases = {
            "top-level": b'{"Id":"one","Id":"two"}',
            "command": b'{"Config":{"Cmd":null,"Cmd":[]}}',
            "entrypoint": b'{"Config":{"Entrypoint":["one"],"Entrypoint":["two"]}}',
            "mount": b'{"Mounts":[{"Destination":"/tmp","Destination":"/workspace"}]}',
            "volume-options": b'{"Options":{"size":"1m","size":"2m"}}',
            "error-response": b'{"message":"first","message":"second"}',
        }
        for name, raw in cases.items():
            with self.subTest(name=name), self.assertRaises(qualification.QualificationError):
                qualification.strict_inspection_json(raw)

    def test_strict_raw_decoder_rejects_invalid_encoding_nonfinite_and_nonobject(self):
        cases = (b'{"value":NaN}', b'{"value":Infinity}', b'{"value":-Infinity}',
            b'{"value":1e10000}', b'{"value":"\xff"}', b'{"value":1} trailing', b'[]', b'null', b'1')
        for raw in cases:
            with self.subTest(raw=raw), self.assertRaises(qualification.QualificationError):
                qualification.strict_inspection_json(raw)

    def test_strict_raw_decoder_bounds_size_depth_and_total_nodes(self):
        cases = {
            "bytes": b'{"value":"' + b"x" * 1048576 + b'"}',
            "depth": b'{"value":' + b"[" * 34 + b"0" + b"]" * 34 + b"}",
            "nodes": b'{"value":[' + b"0," * 100001 + b"0]}",
        }
        for name, raw in cases.items():
            with self.subTest(name=name), self.assertRaises(qualification.QualificationError):
                qualification.strict_inspection_json(raw)

    def test_complete_authenticated_rejection_has_only_diagnostic_authority(self):
        for framing in ("length", "chunked", "close"):
            bundle, before, after = proof_fixture(framing=framing)
            frozen = deepcopy(bundle)
            with self.subTest(framing=framing):
                proof = qualification.validate_start_rejection(bundle)
                self.assertEqual(proof["successful_candidate_starts"], 0)
                self.assertEqual(proof["natural_candidate_completions"], 0)
                self.assertIsNone(proof["candidate_exit_code"])
                self.assertEqual(proof["daemon_bookkeeping_exit_code"], 127)
                self.assertIs(proof["product_acceptance_authority"], False)
                self.assertEqual(proof["before_full_sha256"], qualification.sha256(qualification.encoded(before)))
                self.assertEqual(proof["after_full_sha256"], qualification.sha256(qualification.encoded(after)))
                self.assertEqual(bundle, frozen)

    def test_status_label_complete_file_or_caller_pass_flag_never_grants_authority(self):
        for missing in ("raw-startup-start-response.bin", "raw-startup-start-response-completion.json",
                "raw-startup-inspect-after-kill-response.bin", "raw-startup-cleanup-state.json"):
            bundle, _, _ = proof_fixture()
            del bundle["artifacts"][missing]
            with self.subTest(missing=missing), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for field in ("start_response", "start_response_receipt"):
            bundle, _, _ = proof_fixture()
            change_process(bundle, lambda value: value.update({field: None}))
            with self.subTest(field=field), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for fake in (None, {"matches": True}, {"passed": True, "status": "start_error"}):
            with self.subTest(fake=fake), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(fake)

    def test_response_receipt_requires_exact_digests_counts_flags_and_scope(self):
        replacements = {"body_bytes": True, "body_sha256": "0" * 64,
            "framing_complete": 1, "eof_observed": False, "durably_retained": False,
            "container_id": "f" * 64, "method": "GET", "request_path": "/wrong/start",
            "endpoint_sha256": "0" * 64, "runtime_sha256": "0" * 64,
            "protocol": "unrelated-proof", "status": True}
        for field, bad in replacements.items():
            bundle, _, _ = proof_fixture()
            change_receipt(bundle, lambda value: value.update({field: bad}))
            with self.subTest(field=field), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for field, bad in (("bytes", True), ("bytes", 0), ("sha256", "0" * 64), ("path", "../other")):
            bundle, _, _ = proof_fixture()
            change_receipt(bundle, lambda value: value["response"].update({field: bad}))
            with self.subTest(descriptor=field, bad=bad), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)

    def test_record_receipt_raw_and_checkpoint_must_agree(self):
        for target in ("raw-startup-start-request.bin", "raw-startup-start-response.bin",
                "raw-startup-start-response-completion.json", "raw-startup-cleanup-state.json"):
            bundle, _, _ = proof_fixture()
            bundle["artifacts"][target] += b" "
            with self.subTest(tampered=target), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        bundle, _, _ = proof_fixture()
        changed = json.loads(bundle["artifacts"]["raw-startup-result.json"])
        changed["start_response"]["body_sha256"] = "0" * 64
        replace_owned(bundle, "raw-startup-result.json", changed)
        with self.assertRaises(qualification.QualificationError):
            qualification.validate_start_rejection(bundle)
        bundle, _, _ = proof_fixture()
        bundle["checkpoints"]["raw-startup-start-response.bin"]["index"] = True
        with self.assertRaises(qualification.QualificationError):
            qualification.validate_start_rejection(bundle)

    def test_request_runtime_recipe_and_dispatch_order_are_independently_bound(self):
        bundle, _, _ = proof_fixture()
        wrong = bundle["artifacts"]["raw-startup-start-request.bin"].replace(b"/start ", b"/restart ")
        replace_artifact(bundle, "raw-startup-start-request.bin", wrong)
        change_receipt(bundle, lambda value: value.update(request=qualification.raw_descriptor(
            "raw-startup-start-request.bin", wrong)))
        with self.assertRaises(qualification.QualificationError):
            qualification.validate_start_rejection(bundle)
        for field, bad in (("engine_version", "29.3.0"), ("engine_git_commit", "wrong"),
                ("image_id", "sha256:" + "f" * 64), ("daemon_id", "other")):
            bundle, _, _ = proof_fixture()
            bundle["runtime"][field] = bad
            with self.subTest(runtime=field), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for bad in ([], ABSENT_ARGV + [""], ["/workspace/other"]):
            bundle, _, _ = proof_fixture()
            bundle["expected_argv"] = bad
            with self.subTest(argv=bad), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for left, right in (("raw-startup-start-response-completion.json", "raw-startup-inspect-after-kill-response.bin"),
                ("raw-startup-prestart-comparison.json", "raw-startup-start-request.bin"),
                ("raw-startup-cleanup-state.json", "raw-startup-process.json")):
            bundle, _, _ = proof_fixture()
            bundle["checkpoints"][left]["index"], bundle["checkpoints"][right]["index"] = (
                bundle["checkpoints"][right]["index"], bundle["checkpoints"][left]["index"])
            with self.subTest(left=left), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        bundle, _, _ = proof_fixture()
        bundle["artifacts"]["another-start-request.bin"] = bundle["artifacts"]["raw-startup-start-request.bin"]
        with self.assertRaises(qualification.QualificationError):
            qualification.validate_start_rejection(bundle)

    def test_only_source_grounded_complete_http400_error_shape_qualifies(self):
        for status in (204, 304, 404, 409, 500):
            bundle, _, _ = proof_fixture(status=status)
            with self.subTest(status=status), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        bodies = (b'{"message":"unrelated configuration failure"}', b'{"message":""}',
            b'{"message":127}', b'{"other":"no such file or directory"}',
            b'{"message":"no such file or directory","extra":true}',
            b'{"message":"no such file or directory","message":"no such file or directory"}',
            b'{"message":"\xff"}', b'{"message":NaN}')
        for body in bodies:
            bundle, _, _ = proof_fixture(body=body)
            with self.subTest(body=body), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)

    def test_all_raw_proof_boundaries_reject_duplicate_inspection_keys(self):
        for label in ("raw-container-created", "raw-container-after-start", "raw-container-cleanup-inspect"):
            bundle, _, _ = proof_fixture()
            name = label + "-stdout.bin"
            malformed = bundle["artifacts"][name][:-1] + b',"Config":{"Cmd":null,"Cmd":[]}}'
            replace_artifact(bundle, name, malformed)
            record = json.loads(bundle["artifacts"][label + ".json"])
            record["stdout"].update(qualification.raw_descriptor(name, malformed))
            replace_owned(bundle, label + ".json", record)
            with self.subTest(label=label), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for name in ("raw-startup-inspect-before-response.bin", "raw-startup-inspect-after-kill-response.bin"):
            bundle, _, _ = proof_fixture()
            replace_artifact(bundle, name, http_response(b'{"Id":"one","Id":"two"}', status=200))
            with self.subTest(engine=name), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)

    def test_no_start_guards_reject_started_restarted_oom_and_type_substitutions(self):
        _, before, after = proof_fixture()
        for field, bad in (("Status", "exited"), ("Running", True), ("Paused", True),
                ("Restarting", True), ("Dead", True), ("OOMKilled", True), ("Pid", 42),
                ("Pid", False), ("ExitCode", 0), ("ExitCode", 127.0), ("Error", "another error"),
                ("StartedAt", "2026-10-03T00:00:01Z"), ("Running", 0)):
            changed = deepcopy(after)
            changed["State"][field] = bad
            bundle, _, _ = proof_fixture(before=before, after=changed)
            with self.subTest(field=field, bad=bad), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for field in ("Error", "StartedAt", "ExitCode", "Running"):
            changed = deepcopy(after)
            del changed["State"][field]
            bundle, _, _ = proof_fixture(before=before, after=changed)
            with self.subTest(missing=field), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for bad in (1, False, 0.0):
            changed = deepcopy(after)
            changed["RestartCount"] = bad
            bundle, _, _ = proof_fixture(before=before, after=changed)
            with self.subTest(restart=bad), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)

    def test_no_candidate_exit_output_or_natural_completion_can_be_inferred(self):
        mutations = (lambda value: value.update(exit_code=127),
            lambda value: value["completion"].update(started=True),
            lambda value: value["completion"].update(natural=True),
            lambda value: value.update(startup_comparison={"matches": True}))
        for number, mutation in enumerate(mutations):
            bundle, _, _ = proof_fixture()
            change_process(bundle, mutation)
            with self.subTest(mutation=number), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for channel in ("stdout", "stderr"):
            bundle, _, _ = proof_fixture()
            name, raw = "raw-startup-" + channel + ".bin", b"HTTP400 is not candidate output"
            replace_artifact(bundle, name, raw)
            change_process(bundle, lambda value: value.update({channel: qualification.raw_descriptor(name, raw)}))
            with self.subTest(channel=channel), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)
        for forbidden in ("raw-startup-wait-response.bin", "raw-startup-inspect-final-response.bin",
                "raw-startup-startup-comparison.json"):
            bundle, _, _ = proof_fixture()
            bundle["artifacts"][forbidden] = b"{}"
            with self.subTest(forbidden=forbidden), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)

    def test_diagnostic_identity_permits_only_typed_oom_start_transition_and_mount_order(self):
        for old, new in ((False, False), (False, None), (None, None)):
            _, before, after = proof_fixture()
            before["HostConfig"]["OomKillDisable"] = old
            after["HostConfig"]["OomKillDisable"] = new
            after["Mounts"].reverse()
            bundle, before, after = proof_fixture(before=before, after=after)
            frozen = deepcopy((before, after, bundle))
            with self.subTest(old=old, new=new):
                comparison = qualification.rejected_start_identity_comparison(before, after,
                    runtime=bundle["runtime"], rejection_proof=bundle)
                self.assertIs(comparison["matches"], True, comparison)
                self.assertIs(comparison["product_acceptance_authority"], False)
                self.assertEqual(comparison["comparison_before_sha256"], comparison["comparison_after_sha256"])
                self.assertEqual(comparison["mounts_before"]["order"], [row["Destination"] for row in before["Mounts"]])
                oom = [row for row in comparison["transformations"] if row["path"] == "HostConfig.OomKillDisable"]
                self.assertEqual(len(oom), int(old is False and new is None))
                self.assertEqual((before, after, bundle), frozen)

    def test_diagnostic_identity_rejects_unbound_proof_phase_runtime_and_views(self):
        bundle, before, after = proof_fixture()
        for proof in (None, {"matches": True}, {"passed": True}, {"proof_sha256": "0" * 64}):
            with self.subTest(proof=proof):
                result = qualification.rejected_start_identity_comparison(before, after,
                    runtime=bundle["runtime"], rejection_proof=proof)
                self.assertIs(result["matches"], False)
        for phase in ("candidate-created-to-exited", "candidate-created-to-prestart", "keeper-created-to-running"):
            with self.subTest(phase=phase):
                result = qualification.rejected_start_identity_comparison(before, after,
                    runtime=bundle["runtime"], rejection_proof=bundle, phase=phase)
                self.assertIs(result["matches"], False)
        runtime = deepcopy(bundle["runtime"])
        runtime["daemon_id"] = "another-daemon"
        self.assertIs(qualification.rejected_start_identity_comparison(before, after,
            runtime=runtime, rejection_proof=bundle)["matches"], False)
        changed = deepcopy(after)
        changed["State"]["FinishedAt"] = "2026-10-03T00:00:05Z"
        self.assertIs(qualification.rejected_start_identity_comparison(before, changed,
            runtime=bundle["runtime"], rejection_proof=bundle)["matches"], False)

    def test_diagnostic_identity_preserves_raw_cmd_unknown_fields_and_nested_order(self):
        _, before, after = proof_fixture()
        before["HostConfig"]["future"] = after["HostConfig"]["future"] = {"ordered": [1, 2]}
        for mutation in (lambda view: view["Config"].update(Cmd=[]),
                lambda view: view["HostConfig"]["future"].update(ordered=[2, 1]),
                lambda view: view["Config"].update(FutureConfig=True),
                lambda view: view["Mounts"][0].update(FutureMount={"value": 1}),
                lambda view: view.update(Name="/other-owned-looking-name")):
            changed = deepcopy(after)
            mutation(changed)
            bundle, initial, final = proof_fixture(before=before, after=changed)
            result = qualification.rejected_start_identity_comparison(initial, final,
                runtime=bundle["runtime"], rejection_proof=bundle)
            self.assertIs(result["matches"], False, result)

    def test_diagnostic_identity_rejects_reverse_missing_numeric_and_boolean_oom(self):
        for old, new in ((None, False), (False, True), (False, 0), (False, 1)):
            _, before, after = proof_fixture()
            before["HostConfig"]["OomKillDisable"], after["HostConfig"]["OomKillDisable"] = old, new
            bundle, before, after = proof_fixture(before=before, after=after)
            with self.subTest(old=old, new=new):
                result = qualification.rejected_start_identity_comparison(before, after,
                    runtime=bundle["runtime"], rejection_proof=bundle)
                self.assertIs(result["matches"], False, result)
        _, before, after = proof_fixture()
        del after["HostConfig"]["OomKillDisable"]
        bundle, before, after = proof_fixture(before=before, after=after)
        self.assertIs(qualification.rejected_start_identity_comparison(before, after,
            runtime=bundle["runtime"], rejection_proof=bundle)["matches"], False)

    def test_later_continuity_keeps_oom_cmd_and_no_start_state_exact(self):
        _, before, after = proof_fixture()
        continuous = deepcopy(after)
        continuous["Mounts"].reverse()
        bundle, _, after = proof_fixture(before=before, after=after, cli_after=continuous)
        result = qualification.rejected_start_identity_comparison(after, continuous,
            runtime=bundle["runtime"], rejection_proof=bundle, phase=qualification.REJECTION_CONTINUITY_PHASE)
        self.assertIs(result["matches"], True, result)
        self.assertTrue(all(row["path"] == "Mounts" for row in result["transformations"]))
        for mutation in (lambda view: view["HostConfig"].update(OomKillDisable=False),
                lambda view: view["Config"].update(Cmd=[]),
                lambda view: view["State"].update(Error="another error"),
                lambda view: view["Config"]["Labels"].update({"gossip.source": "f" * 64})):
            changed = deepcopy(after)
            mutation(changed)
            bundle, _, baseline = proof_fixture(before=before, after=after, cli_after=changed)
            result = qualification.rejected_start_identity_comparison(baseline, changed,
                runtime=bundle["runtime"], rejection_proof=bundle, phase=qualification.REJECTION_CONTINUITY_PHASE)
            self.assertIs(result["matches"], False, result)

    def test_raw_startup_null_command_reaches_fake_dispatch_once_and_cleans_before_qualification(self):
        with TemporaryDirectory(prefix="candidate-v4-raw-offline-") as directory:
            observed = exercise_raw_decoder_boundaries(self, Path(directory) / "control")
            self.assertIsNone(observed["error"], observed["error"])
            self.assertEqual(observed["dispatch_count"], 1)
            self.assertEqual(observed["result"]["status"], "startup-unavailable-control-observed")
            self.assertEqual(len(observed["controller"].created_validations), 1)
            self.assertEqual(len(observed["controller"].volume_validations), 2)
            self.assertLess(observed["events"].index("remove-container"), observed["events"].index("qualify"))

    def test_each_raw_helper_decode_boundary_rejects_duplicates_before_use(self):
        cases = (("raw-volume-created", 0, 0), ("raw-container-created", 0, 0),
            ("raw-container-after-start", 1, 1), ("raw-volume-cleanup-inspect", 1, 1))
        for label, dispatches, created_validations in cases:
            with self.subTest(label=label), TemporaryDirectory(prefix="candidate-v4-raw-offline-") as directory:
                root = Path(directory) / "control"
                observed = exercise_raw_decoder_boundaries(self, root, malformed_label=label)
                self.assertIsNotNone(observed["error"])
                self.assertIsNone(observed["result"])
                self.assertEqual(observed["dispatch_count"], dispatches)
                self.assertEqual(len(observed["controller"].created_validations), created_validations)
                terminal = json.loads((root / "raw-transport-terminal.json").read_bytes())
                if label == "raw-volume-cleanup-inspect":
                    self.assertIs(terminal["cleanup"]["volume"], False)
                    self.assertTrue(terminal["cleanup_errors"])
                    self.assertNotIn("raw-volume-remove", observed["controller"].commands)
                if label == "raw-volume-created":
                    self.assertNotIn("raw-container-create", observed["controller"].commands)
                if label in ("raw-volume-created", "raw-volume-cleanup-inspect"):
                    self.assertEqual(len(observed["controller"].volume_validations), 1)

    def test_raw_startup_missing_completion_receipt_cleans_without_qualifying_or_retry(self):
        with TemporaryDirectory(prefix="candidate-v4-raw-offline-") as directory:
            root = Path(directory) / "control"
            observed = exercise_raw_decoder_boundaries(self, root, missing_proof=True)
            self.assertIsInstance(observed["error"], qualification.QualificationError)
            self.assertIsNone(observed["result"])
            self.assertEqual(observed["dispatch_count"], 1)
            terminal = json.loads((root / "raw-transport-terminal.json").read_bytes())
            self.assertEqual(terminal["cleanup"], {"container": True, "volume": True})
            self.assertEqual(terminal["successfully_started_candidate_processes"], 0)
            self.assertEqual(terminal["naturally_completed_candidate_processes"], 0)

    def test_actual_final_qualifier_authenticates_retained_cleanup_and_checkpoint(self):
        with TemporaryDirectory(prefix="candidate-v4-final-offline-") as directory:
            root = Path(directory) / "control"
            controller, record, kwargs = retained_qualification_fixture(root)
            with patch.object(qualification.transport.socket, "socket", side_effect=AssertionError("No socket permitted")), \
                    patch.object(qualification.transport.subprocess, "Popen", side_effect=AssertionError("No subprocess permitted")):
                result = qualification.qualify_rejected_start(self, controller, root, record, **kwargs)
            self.assertEqual(set(result["owned_cleanup"]), {
                "raw-container-remove", "raw-container-absent", "raw-volume-remove", "raw-volume-after"})
            self.assertIs(result["product_acceptance_authority"], False)
            self.assertEqual(result["purpose"], "harness_qualification")
            self.assertEqual(controller.retained_names[-1], "raw-startup-qualification.json")
            self.assertEqual(json.loads((root / "execution" / "raw-startup-qualification.json").read_bytes()), result)

    def test_actual_final_qualifier_rejects_incomplete_cleanup_removal_and_checkpoint(self):
        def edit_command(state, label, change):
            bundle = state["bundle"]
            value = json.loads(bundle["artifacts"][label + ".json"])
            change(value)
            replace_owned(bundle, label + ".json", value)
        def absent_not_empty(state, label):
            bundle = state["bundle"]
            path = label + "-stdout.bin"
            raw = b"still-owned-and-present\n"
            replace_artifact(bundle, path, raw)
            edit_command(state, label, lambda record: record["stdout"].update(qualification.raw_descriptor(path, raw)))
        def missing_removal(state):
            name = "raw-container-remove.json"
            del state["bundle"]["artifacts"][name]
            del state["bundle"]["checkpoints"][name]
        def wrong_checkpoint_hash(state):
            state["before_checkpoint"]["files"][0][1] = "0" * 64
        def late_checkpoint(state):
            name = "raw-startup-intent.json"
            state["before_checkpoint"]["files"].append([name, state["bundle"]["checkpoints"][name]["sha256"]])
        def reverse_cleanup_order(state, first, second):
            checkpoints = state["bundle"]["checkpoints"]
            checkpoints[first]["index"], checkpoints[second]["index"] = (
                checkpoints[second]["index"], checkpoints[first]["index"])
        cases = {
            "container-cleanup-false": lambda state: state["terminal"]["cleanup"].update(container=False),
            "volume-cleanup-false": lambda state: state["terminal"]["cleanup"].update(volume=False),
            "cleanup-error": lambda state: state["terminal"].update(cleanup_errors=["owned removal failed"]),
            "missing-removal": missing_removal,
            "failed-container-removal": lambda state: edit_command(state, "raw-container-remove", lambda value: value.update(exit_code=1)),
            "failed-volume-removal": lambda state: edit_command(state, "raw-volume-remove", lambda value: value.update(exit_code=1)),
            "incomplete-removal-capture": lambda state: edit_command(state, "raw-container-remove", lambda value: value.update(capture_complete=False)),
            "container-still-present": lambda state: absent_not_empty(state, "raw-container-absent"),
            "volume-still-present": lambda state: absent_not_empty(state, "raw-volume-after"),
            "wrong-removal-id": lambda state: edit_command(state, "raw-container-remove", lambda value: value["argv"].__setitem__(-1, "f" * 64)),
            "wrong-volume-name": lambda state: edit_command(state, "raw-volume-remove", lambda value: value["argv"].__setitem__(-1, "another-volume")),
            "wrong-absence-filter": lambda state: edit_command(state, "raw-container-absent", lambda value: value["argv"].__setitem__(-1, "name=^/another$")),
            "missing-before-checkpoint": lambda state: state.update(before_checkpoint=None),
            "before-checkpoint-no-created": lambda state: state["before_checkpoint"].update(files=[]),
            "wrong-before-checkpoint-hash": wrong_checkpoint_hash,
            "before-checkpoint-after-start": late_checkpoint,
            "container-removal-before-cleanup-inspection": lambda state: reverse_cleanup_order(
                state, "raw-container-cleanup-inspect.json", "raw-container-remove.json"),
            "volume-removal-before-ownership-inspection": lambda state: reverse_cleanup_order(
                state, "raw-volume-cleanup-inspect.json", "raw-volume-remove.json"),
        }
        rejected = (AssertionError, qualification.QualificationError, KeyError, FileNotFoundError)
        for name, mutation in cases.items():
            with self.subTest(name=name), TemporaryDirectory(prefix="candidate-v4-final-offline-") as directory:
                root = Path(directory) / "control"
                controller, record, kwargs = retained_qualification_fixture(root, mutation=mutation)
                with patch.object(qualification.transport.socket, "socket", side_effect=AssertionError("No socket permitted")), \
                        patch.object(qualification.transport.subprocess, "Popen", side_effect=AssertionError("No subprocess permitted")), \
                        self.assertRaises(rejected):
                    qualification.qualify_rejected_start(self, controller, root, record, **kwargs)
                self.assertNotIn("raw-startup-qualification.json", controller.retained_names)
                self.assertFalse((root / "execution" / "raw-startup-qualification.json").exists())

    def test_cleanup_identity_requires_exact_id_source_fixture_image_and_argv(self):
        _, before, after = proof_fixture()
        mutations = {
            "wrong-id": lambda view: view.update(Id="f" * 64),
            "missing-id": lambda view: view.pop("Id"),
            "wrong-source": lambda view: view["Config"]["Labels"].update({"gossip.source": "f" * 64}),
            "missing-source": lambda view: view["Config"]["Labels"].pop("gossip.source"),
            "wrong-fixture": lambda view: view["Config"]["Labels"].update({"gossip.fixture": "f" * 64}),
            "missing-fixture": lambda view: view["Config"]["Labels"].pop("gossip.fixture"),
            "wrong-image": lambda view: view.update(Image="sha256:" + "f" * 64),
            "missing-image": lambda view: view.pop("Image"),
            "wrong-executable": lambda view: view.update(Path="/workspace/other"),
            "missing-argv": lambda view: view.pop("Args"),
            "added-empty-argument": lambda view: (view.update(Args=[""]), view["Config"].update(Cmd=[""])),
        }
        for name, mutation in mutations.items():
            changed = deepcopy(after)
            mutation(changed)
            bundle, _, _ = proof_fixture(before=before, after=changed)
            with self.subTest(name=name), self.assertRaises(qualification.QualificationError):
                qualification.validate_start_rejection(bundle)

    def test_composed_raw_helper_real_transport_and_qualifier_require_complete_start_proof(self):
        for lose_start_eof in (False, True):
            with self.subTest(lose_start_eof=lose_start_eof), TemporaryDirectory(prefix="candidate-v4-composed-offline-") as directory:
                root = Path(directory).resolve() / "control"
                observed = composed_raw_control_fixture(self, root, lose_start_eof=lose_start_eof)
                self.assertEqual(observed["state"]["starts"], 1)
                self.assertEqual(observed["state"]["kills"], 1)
                self.assertFalse(observed["state"]["container_present"])
                self.assertFalse(observed["state"]["volume_present"])
                self.assertTrue(all(fake.closed for fake in observed["state"]["sockets"]))
                journal = root / "execution"
                terminal = json.loads((root / "raw-transport-terminal.json").read_bytes())
                self.assertEqual(terminal["cleanup"], {"container": True, "volume": True})
                self.assertEqual(terminal["cleanup_errors"], [])
                self.assertEqual(terminal["successfully_started_candidate_processes"], 0)
                self.assertEqual(terminal["naturally_completed_candidate_processes"], 0)
                record = json.loads((journal / "raw-startup-process.json").read_bytes())
                self.assertEqual(record["status"], "start_error")
                self.assertIsNone(record["exit_code"])
                self.assertIs(record["completion"]["started"], False)
                self.assertIs(record["completion"]["natural"], False)
                self.assertEqual((journal / "raw-startup-start-response.bin").read_bytes(),
                    http_response(qualification.encoded({"message": MESSAGE}), status=400))
                intent = json.loads((journal / "raw-startup-intent.json").read_bytes())
                self.assertEqual(intent["expected_argv"], ABSENT_ARGV)
                created = qualification.retained_command_json(self, journal, "raw-container-created")
                self.assertIs(created["Config"]["Cmd"], None)
                self.assertEqual(created["Config"]["Entrypoint"], ABSENT_ARGV)
                self.assertEqual(created["Args"], [])
                for channel in ("stdout", "stderr"):
                    self.assertEqual((journal / record[channel]["path"]).read_bytes(), b"")
                self.assertFalse(any("/wait?" in path for _, path in observed["state"]["http"]))
                if lose_start_eof:
                    self.assertIsInstance(observed["error"], qualification.QualificationError)
                    self.assertIsNone(observed["result"])
                    self.assertIsNone(record["start_response"])
                    self.assertIsNone(record["start_response_receipt"])
                    self.assertFalse((journal / "raw-startup-start-response-completion.json").exists())
                    self.assertFalse((journal / "raw-startup-qualification.json").exists())
                else:
                    self.assertIsNone(observed["error"], observed["error"])
                    self.assertEqual(observed["result"]["status"], "startup-unavailable-control-observed")
                    self.assertEqual(record["start_response"]["status"], 400)
                    qualified = json.loads((journal / "raw-startup-qualification.json").read_bytes())
                    self.assertIs(qualified["product_acceptance_authority"], False)
                    self.assertEqual(qualified["proof"]["successful_candidate_starts"], 0)
                    self.assertEqual(qualified["proof"]["natural_candidate_completions"], 0)
                    self.assertEqual(set(qualified["owned_cleanup"]), {
                        "raw-container-remove", "raw-container-absent", "raw-volume-remove", "raw-volume-after"})
