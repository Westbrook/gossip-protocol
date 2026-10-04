"""Prospective intact HTTP/CLI physical mechanics qualification; no product oracle.

Dispatch requires the separately reviewed finite v4 persistence and eleven
controls. Root freezes and verifies those prerequisites before the Docker lane.
Offline classes inspect declarations, inert source and supplied transcripts;
they never execute fixture source or substitute for physical evidence.
"""
from __future__ import annotations

import ast
from dataclasses import asdict, replace
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_cases_v1 as cli_cases
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_execution_v3 as execution
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness.gitstore import GitStore
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from tests import candidate_http_v3_mixed_fixture as fixture
from tests import test_candidate_clients_docker_v4 as finite_checks
from tests import test_candidate_http_docker_v3 as raw_checks

PURPOSE = "harness_qualification"


def policy():
    return execution.HttpPolicy(RUNTIME_IMAGE, wire_limits=wire.WireLimits(
        response_limit_bytes=65536, timeout_seconds=5.0), lifetime_seconds=7200)


def control_definition():
    recipe = fixture.fixed_mixed_recipe()
    return {"control_id": "X01", "name": "intact-87-step-http-cli-http-state-namespace",
            "recipe": recipe.record(), "expected": list(fixture.expected_observations(recipe)),
            "policy": asdict(policy()), "purpose": PURPOSE, "step_count": 87,
            "request_count": 54, "cli_count": 29, "server_epoch_count": 2,
            "planned_container_count": 86, "required_prior_finite_gate": "v4-persistence-and-all-eleven-controls",
            "complete_v3_gate": False, "acceptance_credit": False,
            "product_semantics_implemented": False, "retries": 0}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def decode_attach_transcript(raw, *, stream_limit, frame_limit, eof_after_start):
    """Independent bounded HTTP upgrade and Docker multiplex parser, without I/O.

    Socket EOF must be independently retained by the transport; a file ending
    after a complete frame cannot supply that missing observation.
    """
    _require(type(raw) is bytes and eof_after_start is True, "Observed post-start attach EOF required")
    _require(type(stream_limit) is int and type(frame_limit) is int
             and stream_limit > 0 and frame_limit > 0, "Positive stream/frame limits required")
    marker = raw.find(b"\r\n\r\n")
    _require(0 <= marker and marker + 4 <= 32768, "Missing or excessive attach head")
    lines = raw[:marker].split(b"\r\n")
    _require(re.fullmatch(rb"HTTP/1\.[01] 101 [\x20-\x7e\x80-\xff]*", lines[0]) is not None,
             "Attach upgrade status differs")
    headers = {}
    _require(len(lines) <= 128, "Excessive attach headers")
    for line in lines[1:]:
        _require(b":" in line and line[:1] not in (b" ", b"\t"), "Malformed attach header")
        name, value = line.split(b":", 1)
        _require(re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is not None,
                 "Malformed attach header name")
        name = name.decode("ascii").lower()
        _require(name not in headers and all((x >= 32 and x != 127) or x == 9 for x in value),
                 "Duplicate or malformed attach header value")
        headers[name] = value.decode("latin1").strip(" \t")
    _require(headers.get("connection", "").lower() == "upgrade"
             and headers.get("upgrade", "").lower() == "tcp"
             and headers.get("content-type") in ("application/vnd.docker.raw-stream",
                                                  "application/vnd.docker.multiplexed-stream")
             and "content-length" not in headers and "transfer-encoding" not in headers,
             "Attach upgrade headers differ")
    streams = {1: bytearray(), 2: bytearray()}
    offset, frames = marker + 4, 0
    while offset < len(raw):
        _require(offset + 8 <= len(raw), "Incomplete Docker frame header")
        header = raw[offset:offset + 8]
        channel, size = header[0], int.from_bytes(header[4:], "big")
        _require(channel in streams and header[1:4] == b"\0\0\0", "Invalid Docker frame channel/reserved bytes")
        _require(size <= frame_limit and offset + 8 + size <= len(raw), "Incomplete or excessive Docker frame")
        frames += 1
        _require(frames <= 65536 and len(streams[channel]) + size <= stream_limit,
                 "Docker frame count or stream cap exceeded")
        streams[channel].extend(raw[offset + 8:offset + 8 + size])
        offset += 8 + size
    return {"stdout": bytes(streams[1]), "stderr": bytes(streams[2]), "frames": frames}


def _timestamp(value):
    match = re.fullmatch(r"([1-9][0-9]{3}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z", value)
    if match is None:
        raise ValueError("Missing actual runtime timestamp")
    return datetime.fromisoformat(match[1] + "+00:00"), int((match[2] or "0").ljust(9, "0"))


def assert_cli_process(test, journal, recipe, bounds, process, index, *, first_seen):
    """Authenticate retained finite process independently of later keeper outcome.

    The caller first authenticates this history's checkpoint membership/digests.
    This helper is also usable when a real successful CLI process is followed by
    a continuity or observation-retention failure. It does not require a success
    row and grants no semantic or whole-history authority.
    """
    label = "step-" + str(index).zfill(3)
    prefix = label + "-cli-process"
    step = recipe.steps[index]
    test.assertEqual(step.kind, "cli")
    config = raw_checks.read_journal(journal / "config.json")
    binding = config["registration"]["binding"]
    intent = raw_checks.read_journal(journal / "intent.json")
    creation = raw_checks.read_journal(journal / (label + "-cli-create-intent.json"))
    spec = raw_checks.role_spec(creation["spec"])
    test.assertEqual(spec.role, "cli")
    test.assertEqual(spec.name, intent["containers"][index])
    cli_intent = raw_checks.read_journal(journal / (label + "-cli-intent.json"))
    keeper_baseline = raw_checks.retained_engine_json(test, journal, "keeper-running")
    expected_intent = {"step_id": step.step_id, "step_index": index, "kind": "cli", "label": label,
        "root_path": step.root_path, "step_sha256": execution.digest(step.record()),
        "definition_sha256": recipe.definition_sha256, "row_id": recipe.row_id or recipe.recipe_id,
        "epoch": step.epoch, "database_volume": intent["volume"], "database_path": recipe.database_path,
        "source_sha256": binding["source_sha256"], "fixture_sha256": binding["fixture_sha256"],
        "keeper_id": keeper_baseline["Id"], "binding": binding, "authenticated": False,
        "expected_argv": list(step.argv), "product_verdict": "not_evaluated"}
    test.assertEqual(raw_checks.encoded(cli_intent), raw_checks.encoded(expected_intent))
    test.assertEqual(spec.argv, step.argv)
    test.assertEqual(spec.volume, intent["volume"])
    staging = raw_checks.read_journal(journal / "staging.json")
    test.assertEqual(staging["source_manifest"], config["source_manifest"])
    test.assertEqual(staging["input_manifest"], recipe.record()["input_manifest"])
    test.assertEqual(spec.binds, (("/workspace", staging["workspace"]), ("/inputs", staging["inputs"])))
    test.assertEqual(binding["source_sha256"], execution.digest({"protocol": execution.PROTOCOL, "files": staging["source_manifest"]}))
    test.assertEqual(binding["fixture_sha256"], execution.digest(staging["input_manifest"]))
    test.assertEqual(spec.labels, tuple(sorted({"gossip.execution": intent["execution_id"],
        "gossip.role": "cli", "gossip.source": binding["source_sha256"],
        "gossip.fixture": binding["fixture_sha256"], "gossip.epoch": str(step.epoch),
        "gossip.step": step.step_id, "gossip.helper": binding["helper_sha256"]}.items())))
    created = raw_checks.retained_engine_json(test, journal, label + "-cli-created")
    before = raw_checks.retained_engine_json(test, journal, prefix + "-inspect-before")
    final = raw_checks.retained_engine_json(test, journal, prefix + "-inspect-final")
    container_id = created["Id"]
    test.assertRegex(container_id, r"^[0-9a-f]{64}$")
    for value in (created, before, final):
        test.assertEqual(value["Id"], container_id)
        execution.validate_role(value, spec, RUNTIME_IMAGE, test.runtime)
        test.assertEqual(value["Config"]["Entrypoint"], [step.argv[0]])
        test.assertEqual(value["Config"]["Cmd"], list(step.argv[1:]))
        test.assertEqual((value["Path"], value["Args"]), (step.argv[0], list(step.argv[1:])))
        test.assertEqual(value["Config"]["Labels"], dict(spec.labels))
        mounts = value["Mounts"]
        test.assertEqual(len(mounts), 3)
        by_path = {row["Destination"]: row for row in mounts}
        test.assertEqual(set(by_path), {"/workspace", "/inputs", "/tmp"})
        test.assertEqual(by_path["/tmp"]["Name"], intent["volume"])
        test.assertIs(by_path["/tmp"]["RW"], True)
        for path, source in spec.binds:
            test.assertEqual(by_path[path]["Source"], source)
            test.assertIs(by_path[path]["RW"], False)
    for value in (created, before):
        execution.validate_state(value, "created")
    execution.validate_state(final, "exited")
    process_intent = raw_checks.read_journal(journal / (prefix + "-intent.json"))
    test.assertEqual(process_intent["container_id"], container_id)
    test.assertEqual(process_intent["protocol"], engine.PROTOCOL)
    test.assertEqual(process_intent["expected_argv"], list(step.argv))
    test.assertEqual(process_intent["expected_argv_sha256"], raw_checks.sha256(finite_checks.encoded(list(step.argv))))
    test.assertEqual(process_intent["expected_runtime"], test.runtime)
    test.assertEqual(process_intent["sources"], engine.evaluator_sources())
    process_policy = engine.ProcessPolicy(bounds.image_id, timeout_seconds=bounds.cli_timeout_seconds,
        stream_limit_bytes=bounds.cli_stream_limit_bytes, frame_limit_bytes=bounds.cli_stream_limit_bytes,
        transport_timeout_seconds=bounds.transport_timeout_seconds)
    test.assertEqual(process_intent["policy"], asdict(process_policy))
    test.assertEqual(process_intent["fixed_limits"], {"headers": 32768, "control_body": 1048576, "frame_count": 65536})
    for name, definition in (("command", engine.command_policy()), ("start_response", engine.start_response_policy()),
                             ("startup", engine.startup_policy()), ("identity", engine.identity_policy())):
        test.assertEqual(process_intent[name + "_policy"], definition)
        test.assertEqual(process_intent[name + "_policy_sha256"],
                         raw_checks.sha256(finite_checks.encoded(definition)))
    created_projection = finite_checks.inspection_projection(test, created, "candidate")
    test.assertEqual(process_intent["expected_inspection_sha256"],
                     raw_checks.sha256(finite_checks.encoded(created_projection)))
    test.assertEqual(process_intent["expected_full_inspection_sha256"],
                     raw_checks.sha256(finite_checks.encoded(created)))
    for suffix, key, after, phase in (("prestart-comparison", "prestart_comparison", before, "candidate-created-to-prestart"),
                                      ("startup-comparison", "startup_comparison", final, "candidate-created-to-exited")):
        comparison = raw_checks.read_journal(journal / (prefix + "-" + suffix + ".json"))
        test.assertEqual(comparison, process[key])
        finite_checks.assert_identity_comparison(test, comparison, created_projection,
            finite_checks.inspection_projection(test, after, "candidate"), test.runtime, phase, created, after)

    runtime_prefix = prefix + "-runtime-before"
    version, info, image = (raw_checks.retained_engine_json(test, journal, runtime_prefix + "-" + part)
                            for part in ("version", "info", "image"))
    actual_runtime = {"protocol": engine.PROTOCOL, "endpoint": config["endpoint"], "api_version": "1.47",
        "os": version["Os"], "engine_git_commit": version["GitCommit"], "cgroup_version": info["CgroupVersion"],
        "cgroup_driver": info["CgroupDriver"], "oom_kill_disable_supported": info["OomKillDisable"],
        "daemon_id": info["ID"], "engine_version": version["Version"], "architecture": version["Arch"],
        "kernel_version": version["KernelVersion"], "image_id": image["Id"],
        "image_inspect_sha256": raw_checks.sha256(finite_checks.encoded(image))}
    test.assertEqual(actual_runtime, test.runtime)
    test.assertEqual(raw_checks.read_journal(journal / (runtime_prefix + ".json")), actual_runtime)
    test.assertEqual(info["OSType"], "linux")
    test.assertEqual(image["Os"], "linux")
    test.assertLessEqual(tuple(map(int, version["MinAPIVersion"].split("."))), (1, 47))
    test.assertGreaterEqual(tuple(map(int, version["ApiVersion"].split("."))), (1, 47))

    def read(name):
        test.assertIn(name, first_seen)
        return raw_checks.journal_reader.read(journal / name)

    target = "/v1.47/containers/" + container_id
    request = ("POST " + target + "/start HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode()
    test.assertEqual(read(prefix + "-start-request.bin"), request)
    for suffix, method, path in (("wait", "POST", target + "/wait?condition=not-running"),
            ("inspect-before", "GET", target + "/json"), ("inspect-final", "GET", target + "/json"),
            ("runtime-before-version", "GET", "/v1.47/version"),
            ("runtime-before-info", "GET", "/v1.47/info"),
            ("runtime-before-image", "GET", "/v1.47/images/" + bounds.image_id + "/json")):
        test.assertEqual(read(prefix + "-" + suffix + "-request.bin"),
            (method + " " + path + " HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode())
    # A single declared start request exists for this full container ID. Other
    # history processes have their own distinct IDs and cannot satisfy it.
    start_line = ("POST " + target + "/start HTTP/1.1").encode()
    start_records = [name for name in first_seen if name.endswith("-request.bin")
                     and read(name).split(b"\r\n", 1)[0] == start_line]
    test.assertEqual(start_records, [prefix + "-start-request.bin"])
    test.assertEqual(read(prefix + "-attach-request.bin"), ("POST " + target
        + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0 HTTP/1.1\r\nHost: docker\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n").encode())
    receipt = raw_checks.strict_control_json(raw_checks.retained_bytes(test, journal, process["start_response_receipt"]))
    test.assertEqual(process["start_response_receipt"]["path"], prefix + "-start-response-completion.json")
    test.assertEqual(receipt, process["start_response"])
    test.assertEqual(receipt["protocol"], engine.START_RESPONSE_PROTOCOL)
    test.assertEqual((receipt["method"], receipt["request_path"], receipt["container_id"]), ("POST", target + "/start", container_id))
    test.assertEqual(receipt["endpoint_sha256"], raw_checks.sha256(finite_checks.encoded(config["endpoint"])))
    test.assertEqual(receipt["runtime_sha256"], raw_checks.sha256(finite_checks.encoded(test.runtime)))
    for field in ("framing_complete", "eof_observed", "durably_retained"):
        test.assertIs(receipt[field], True)
    test.assertEqual(receipt["request"]["path"], prefix + "-start-request.bin")
    test.assertEqual(receipt["response"]["path"], prefix + "-start-response.bin")
    test.assertEqual(raw_checks.retained_bytes(test, journal, receipt["request"]), request)
    response = finite_checks.parse_retained_http(raw_checks.retained_bytes(test, journal, receipt["response"]),
                                                eof_observed=receipt["eof_observed"])
    test.assertEqual((receipt["status"], response["status"], response["body"]), (204, 204, b""))
    test.assertEqual((receipt["body_bytes"], receipt["body_sha256"]), (0, raw_checks.sha256(b"")))
    attach = decode_attach_transcript(read(prefix + "-attach-response.bin"),
        stream_limit=bounds.cli_stream_limit_bytes, frame_limit=bounds.cli_stream_limit_bytes,
        eof_after_start=process["attach_eof_after_start_confirmation"])
    test.assertIs(process["capture_complete"], True)
    test.assertEqual(process["frames"], attach["frames"])
    for name in ("stdout", "stderr"):
        descriptor = process[name]
        test.assertEqual(descriptor["path"], prefix + "-" + name + ".bin")
        test.assertIs(descriptor["truncated"], False)
        test.assertIs(descriptor["complete"], True)
        test.assertEqual(descriptor["observed_bytes"], len(attach[name]))
        test.assertEqual(raw_checks.retained_bytes(test, journal, descriptor), attach[name])
    test.assertEqual(attach["stderr"], b"")
    wait = raw_checks.retained_engine_json(test, journal, prefix + "-wait")
    test.assertIs(type(wait["StatusCode"]), int)
    test.assertEqual(wait["StatusCode"], 0)
    test.assertTrue(wait.get("Error") is None or wait["Error"] == {"Message": ""})
    state = final["State"]
    test.assertIs(type(state["ExitCode"]), int)
    test.assertEqual(state["ExitCode"], 0)
    for key in ("Running", "Paused", "Restarting", "Dead", "OOMKilled"):
        test.assertIs(state[key], False)
    test.assertEqual((state["Status"], state["Error"], state["Pid"], final["RestartCount"]), ("exited", "", 0, 0))
    test.assertIs(type(state["Pid"]), int)
    test.assertIs(type(final["RestartCount"]), int)
    test.assertLessEqual(_timestamp(final["Created"]), _timestamp(state["StartedAt"]))
    test.assertLessEqual(_timestamp(state["StartedAt"]), _timestamp(state["FinishedAt"]))
    test.assertFalse(any(name.startswith(prefix + "-kill-") for name in first_seen))
    completion = process["completion"]
    test.assertEqual(completion, {"container_id": container_id, "started": True, "natural": True,
        "wait_status_code": 0, "inspect_exit_code": 0, "finished_at": state["FinishedAt"],
        "started_at": state["StartedAt"], "oom_killed": False, "state_error": "",
        "killed_by_controller": False, "identity_verified": True, "wait_error": wait.get("Error"),
        "signal_exit_ambiguous": False})
    test.assertEqual((process["protocol"], process["status"], process["exit_code"], process["error"]),
                     (engine.PROTOCOL, "completed", 0, None))
    test.assertEqual(raw_checks.read_journal(journal / (label + "-cli-process-result.json")), process)
    test.assertEqual(raw_checks.read_journal(journal / (prefix + "-process.json")), process)
    evidence = sorted((name for name in first_seen if name.startswith(prefix + "-")
                       and name not in (label + "-cli-process-result.json", prefix + "-process.json")),
                      key=first_seen.__getitem__)
    test.assertEqual(process["evidence"], evidence)
    names = [label + "-cli-intent.json", label + "-cli-create-intent.json", label + "-cli-created-response.bin",
             prefix + "-intent.json", prefix + "-inspect-before-response.bin", prefix + "-prestart-comparison.json",
             prefix + "-attach-request.bin", prefix + "-start-request.bin", prefix + "-start-response-completion.json",
             prefix + "-wait-response.bin", prefix + "-inspect-final-response.bin", prefix + "-attach-response.bin",
             prefix + "-process.json", label + "-cli-process-result.json"]
    for left, right in zip(names, names[1:]):
        test.assertLess(first_seen[left], first_seen[right], (left, right))
    return {"stdout": attach["stdout"], "stderr": attach["stderr"], "container_id": container_id}


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateHttpMixedV3DockerTests(raw_checks.HttpV3PhysicalAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-c03-http-mixed-v3", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.outcome = {"control_id": "X01", "status": "not-run", "acceptance_credit": False}
        cls.addClassCleanup(cls.save_census)
        cls.recipe, cls.bounds = fixture.fixed_mixed_recipe(), policy()
        cls.definition = control_definition()
        cls.endpoint = engine.EngineEndpoint.from_environment()

        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())

        cls.runtime = engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=retain_runtime, label="class-runtime")
        cls.files = fixture.source_files()
        cls.store = GitStore.create(cls.artifacts.root / "fixture.git",
                                   {name: raw.decode("utf-8") for name, raw in cls.files.items()})
        cls.commit = cls.store.head()
        cls.tree, captured = execution.capture_git_source(cls.store, cls.commit)
        if captured != cls.files:
            raise AssertionError("Committed inert fixture differs from authored source")
        cls.requirements_sha256 = cli_cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
        cls.roster_sha256 = raw_checks.sha256(raw_checks.encoded([cls.definition]))
        raw_checks.write_new(cls.artifacts.root / "prospective-controls.json", {
            "controls": [cls.definition], "ordered_roster_sha256": cls.roster_sha256,
            "source_commit": cls.commit, "source_tree": cls.tree,
            "source_files": {name: raw_checks.sha256(raw) for name, raw in cls.files.items()},
            "source_sha256": execution.source_sha256(cls.files), "runtime": cls.runtime,
            "helper_sha256": wire.helper_sha256(), "evaluator_sources": execution.evaluator_sources(),
            "definition_sources": {Path(module.__file__).name: raw_checks.sha256(Path(module.__file__).read_bytes())
                for module in (fixture, raw_checks, finite_checks, raw_checks.control_framing)},
            "control_source_sha256": raw_checks.sha256(Path(__file__).read_bytes()),
            "purpose": PURPOSE, "complete_v3_gate": False, "acceptance_credit": False})

    @classmethod
    def save_census(cls):
        raw_checks.write_new(cls.artifacts.root / "completion-census.json", {
            "planned": ["X01"], "outcomes": [cls.outcome],
            "not_run": ["X01"] if cls.outcome["status"] == "not-run" else [],
            "not_qualified": ["X01"] if cls.outcome["status"] != "qualified" else [],
            "complete_v3_gate": False, "production_acceptance_authority": False,
            "acceptance_credit": False, "full_requirement_verdict": None})

    def test_x01_intact_eighty_seven_steps_and_cross_interface_state(self):
        self.assertEqual(self.outcome["status"], "not-run", "Never redispatch this control")
        self.outcome["status"] = "started-not-qualified"
        root = self.artifacts.root / "x01"
        root.mkdir()
        recipe, bounds = self.recipe, self.bounds
        binding = execution.binding_for(self.files, recipe, bounds, self.runtime,
            requirements_sha256=self.requirements_sha256)
        registration = execution.HttpRegistration(binding, self.commit, self.tree, "physical-http-mixed-v3-1")
        raw_checks.write_new(root / "prospective-registration.json", {"registration": asdict(registration),
            "definition": self.definition, "ordered_roster_sha256": self.roster_sha256, "purpose": PURPOSE})
        checkpoints = []

        def checkpoint_sink(checkpoint):
            raw_checks.write_new(root / ("external-checkpoint-" + str(len(checkpoints)).zfill(5) + ".json"), asdict(checkpoint))
            checkpoints.append(checkpoint)

        journal = root / "execution"
        with execution.CandidateHttpExecution(journal, self.store, registration, recipe, bounds,
                endpoint=self.endpoint, checkpoint_sink=checkpoint_sink) as controller:
            result = controller.execute_once()
            self.assertEqual(result.checkpoint, controller.checkpoint())
        terminal_raw = raw_checks.read_journal(journal / "terminal.json")
        self.outcome.update({"execution_status": result.status, "missing_step_ids": list(result.missing_step_ids),
            "infrastructure": list(result.infrastructure), "cleanup_verified": result.cleanup_verified,
            "terminal_sha256": result.terminal_sha256, "observed_probe_records": len(result.observations),
            "observed_cli_records": len(result.cli_observations), "attempted_probes": terminal_raw["attempted_probes"],
            "attempted_cli": terminal_raw["attempted_cli"], "unknown_request_outcomes": terminal_raw["unknown_request_outcomes"],
            "unavailable_cli_attempts": terminal_raw["unavailable_cli_attempts"]})
        raw_checks.write_new(root / "physical-census.json", self.outcome)
        terminal = self.authenticate_history(journal, recipe, binding, result, checkpoints)
        self.assertEqual((recipe.row_id, recipe.definition_sha256), (fixture.MIXED_ROW_ID, fixture.MIXED_DEFINITION_SHA256))
        self.assertEqual(result.status, "completed", result)
        self.assertEqual(result.missing_step_ids, ())
        self.assertEqual(result.infrastructure, ())
        self.assertIs(result.cleanup_verified, True)
        self.assertEqual(len(terminal["steps"]), 87)
        self.assertEqual(terminal["unentered_step_ids"], [])
        for name, value in {"planned_requests": 54, "attempted_probes": 54, "observed_requests": 54,
                "observed_probe_records": 54, "planned_cli": 29, "attempted_cli": 29,
                "observed_cli_records": 29, "unknown_request_outcomes": 0, "unavailable_cli_attempts": 0,
                "created_helpers": 54, "created_servers": 2, "created_keepers": 1,
                "created_cli": 29, "unconfirmed_creates": 0, "create_intents": 86}.items():
            self.assertEqual(terminal[name], value, name)
        expected = {row["step_id"]: row for row in self.definition["expected"]}
        facts = {}
        for observation in result.observations:
            actual = self.authenticated_wire(journal, recipe, bounds, observation)
            self.assertIs(actual.exchange_complete, True)
            self.assertEqual(actual.response.status_code, 200)
            self.assert_listener(actual, port=recipe.port)
            value = raw_checks.strict_control_json(actual.response.body)
            self.assertEqual(raw_checks.encoded(value), raw_checks.encoded(expected[observation["step_id"]]["json"]))
            facts[observation["step_index"]] = value
        first_seen = terminal["_authenticated_first_seen"]
        keeper = raw_checks.retained_engine_json(self, journal, "keeper-running")
        volume = raw_checks.read_journal(journal / "volume-baseline.json")
        cli_ids = []
        for observation in result.cli_observations:
            index, label = observation["step_index"], observation["label"]
            raw = assert_cli_process(self, journal, recipe, bounds, observation["process"], index, first_seen=first_seen)
            cli_ids.append(raw["container_id"])
            self.assertIs(observation["authenticated"], True)
            self.assertIs(observation["cli_removed"], True)
            self.assertEqual(observation["cli_id"], raw["container_id"])
            self.assertEqual(observation["keeper_id"], keeper["Id"])
            self.assertEqual(observation["database_volume"], volume["Name"])
            self.assertEqual(observation["database_path"], recipe.database_path)
            self.assertEqual(observation["expected_argv"], list(recipe.steps[index].argv))
            self.assertEqual(observation["product_verdict"], "not_evaluated")
            self.assertEqual(raw["stdout"], execution.encoded(expected[observation["step_id"]]["json"]) + b"\n")
            facts[index] = raw_checks.strict_control_json(raw["stdout"])
            for suffix in ("keeper-before", "cli-keeper-prestart", "cli-keeper-after"):
                current = raw_checks.retained_engine_json(self, journal, label + "-" + suffix)
                comparison = raw_checks.read_journal(journal / (label + "-" + suffix + "-continuity.json"))
                self.assert_running_interval(keeper, current, comparison)
                if suffix == "cli-keeper-after":
                    self.assertEqual(comparison, observation["keeper_continuity"])
            for suffix in ("before", "after"):
                record = raw_checks.read_journal(journal / (label + "-cli-volume-" + suffix + ".json"))
                self.assertEqual(record["argv"], ["docker", "--host", "unix://" + self.runtime["endpoint"]["socket_path"],
                    "volume", "inspect", "--format", "{{json .}}", volume["Name"]])
                self.assertEqual(record["exit_code"], 0)
                self.assertIs(record["capture_complete"], True)
                self.assertIs(record["timed_out"], False)
                for stream in ("stdout", "stderr"):
                    descriptor = record[stream]
                    self.assertEqual(descriptor["path"], label + "-cli-volume-" + suffix + "-" + stream + ".bin")
                    self.assertIs(descriptor["truncated"], False)
                    self.assertEqual(descriptor["observed_bytes"], descriptor["bytes"])
                self.assertEqual(raw_checks.retained_bytes(self, journal, record["stderr"]), b"")
                value = raw_checks.strict_control_json(raw_checks.retained_bytes(self, journal, record["stdout"]))
                self.assertEqual(raw_checks.encoded(value), raw_checks.encoded(volume))
            previous_absence = "step-" + str(index - 1).zfill(3) + ("-retire-absence.json" if index == 40 else "-cli-retire-absence.json")
            following_creation = "step-" + str(index + 1).zfill(3) + ("-create-intent.json" if index == 68 else "-cli-create-intent.json")
            order = [previous_absence, label + "-cli-create-intent.json", label + "-cli-keeper-prestart-response.bin",
                label + "-cli-volume-before.json", label + "-cli-process-intent.json", label + "-cli-process-result.json",
                label + "-cli-keeper-after-response.bin", label + "-cli-volume-after.json",
                label + "-cli-retire-absence.json", following_creation]
            for left, right in zip(order, order[1:]):
                self.assertLess(first_seen[left], first_seen[right], (left, right))
        self.assertEqual(len(cli_ids), len(set(cli_ids)))
        other_ids = {keeper["Id"], *(row["server_id"] for row in terminal["epochs"]),
                     *(row["probe_id"] for row in result.observations)}
        self.assertTrue(set(cli_ids).isdisjoint(other_ids))
        first, second = terminal["epochs"]
        self.assertEqual([first["epoch"], second["epoch"]], [1, 2])
        self.assertNotEqual(first["server_id"], second["server_id"])
        for name in ("keeper_id", "keeper_started_at", "database_volume", "database_path", "root_path", "argv", "source_sha256", "fixture_sha256"):
            self.assertEqual(first[name], second[name], name)
        self.assertEqual(facts[38]["after"], facts[40]["before"])
        self.assertEqual(facts[38]["after"]["http"], 38)
        for index in range(40, 68):
            self.assertEqual(facts[index]["after"], facts[index + 1]["before"])
        self.assertEqual({**facts[68]["after"], "starts": 2}, facts[70]["before"])
        self.assertEqual((facts[70]["before"]["http"], facts[70]["before"]["cli"], facts[85]["after"]["events"]), (38, 29, 83))
        self.assertEqual(execution.capture_git_source(self.store, self.commit), (self.tree, self.files))
        self.outcome.update(status="qualified", qualification_verified=True,
            mechanics_assertions={"intact_steps": 87, "actual_http_requests": 54, "actual_finite_cli": 29,
                "same_namespace_state_readback_both_directions": True, "continuous_keeper_and_volume": True,
                "natural_cli_completion_and_removal_order": True, "product_semantics_accepted": False})
        raw_checks.write_new(root / "mechanics-outcome.json", self.outcome)


class CandidateHttpMixedV3DeclarationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recipe = fixture.fixed_mixed_recipe()
        cls.row = next(row for row in catalog.definitions() if row.row_id == fixture.MIXED_ROW_ID)

    def test_exact_pinned_row_and_full_literal_order(self):
        self.assertEqual(self.recipe.definition_sha256, "4e571dabbe0f51ed959ed27a59634d3b99d4cdb723e5054b1511a4b795897e4a")
        self.assertEqual(len(self.recipe.steps), 87)
        self.assertEqual([step.kind for step in self.recipe.steps],
            ["start"] + ["probe"] * 38 + ["stop"] + ["cli"] * 29 + ["start"] + ["probe"] * 16 + ["stop"])
        self.assertEqual([step.step_id for step in self.recipe.steps], [step.step_id for step in self.row.steps])
        self.assertEqual(self.recipe.definition_json, core.encode(self.row.record()))
        self.assertEqual(self.recipe.input_entries, self.row.fixtures)
        for source, projected in zip(self.row.steps, self.recipe.steps, strict=True):
            self.assertEqual(projected.declaration_json, core.encode(source.record()))
            self.assertEqual((projected.epoch, projected.root_path, projected.argv), (source.epoch, source.root, source.argv))
            if source.request:
                self.assertEqual(wire.request_bytes(json.loads(projected.request_json), self.recipe.port), source.request.wire_bytes())
        self.assertFalse(self.recipe.record()["acceptance_authority"])

    def test_exact_cli_argv_roster_and_two_server_epochs(self):
        commands = [step.argv[7:] for step in self.recipe.steps if step.kind == "cli"]
        guard = ("show", "doc-2ab2ffad6b0e585e3ce110d6886680c88ed7fe2088aea2910ded1449ef46d024")
        reads = [("jobs",), ("job-show", "sentinel"), ("job-show", "subject"),
                 ("list", "--offset", "0", "--limit", "100"), ("export",), guard]
        self.assertEqual(commands, reads + [("job-retry", "subject")] + reads
            + [("job-prepare", "subject")] + reads + [("job-commit", "subject", "3")]
            + reads[:-1] + [("show", "doc-e33e4f494b80b682de63a64fba092041d0cf073eb7da7bb185c6fb643346fbbb"),
                           ("show", "doc-836a48ac21958447288e5b0be5ab074d8e046c068d0208e7fa513805d8cf507a"), guard])
        prefix = ("python", "-m", "library", "--db", "/tmp/catalog.sqlite", "--root", "/inputs")
        self.assertTrue(all(step.argv[:7] == prefix for step in self.recipe.steps if step.kind in ("start", "cli")))
        self.assertEqual([step.argv for step in self.recipe.steps if step.kind == "start"],
                         [prefix + ("serve", "--port", "18765")] * 2)

    def test_prospective_state_bridges_both_interfaces_without_product_answers(self):
        rows = fixture.expected_observations(self.recipe)
        facts = {row["step_index"]: row["json"] for row in rows}
        self.assertEqual(len(rows), 83)
        self.assertEqual(facts[38]["after"], facts[40]["before"])
        self.assertEqual((facts[40]["before"]["http"], facts[40]["before"]["cli"]), (38, 0))
        self.assertEqual({**facts[68]["after"], "starts": 2}, facts[70]["before"])
        self.assertEqual((facts[85]["after"]["http"], facts[85]["after"]["cli"], facts[85]["after"]["events"]), (54, 29, 83))
        self.assertEqual(facts[40]["before"]["last_event"]["interface"], "http")
        self.assertEqual(facts[70]["before"]["last_event"]["interface"], "cli")
        chain = bytes(32)
        for row in rows:
            event = row["json"]["event"]
            chain = hashlib.sha256(chain + core.encode(event)).digest()
            self.assertEqual(row["json"]["after"]["chain"], chain.hex())
        self.assertEqual(control_definition()["planned_container_count"], 86)

    def test_changed_missing_or_duplicate_row_is_rejected(self):
        changed = replace(self.row, notes=self.row.notes + ("Deliberate definition drift",))
        for rows in ((), (changed,), (self.row, self.row)):
            with self.subTest(count=len(rows)), patch.object(catalog, "definitions", return_value=rows):
                with self.assertRaisesRegex(ValueError, "declaration changed"):
                    fixture.fixed_mixed_recipe()
        with self.assertRaisesRegex(ValueError, "Unknown or changed"):
            fixture.expected_observations(replace(self.recipe, recipe_id="changed"))

    def test_fixture_source_is_inert_and_independent(self):
        tree = ast.parse(fixture.MIXED_SOURCE, filename="library.py")
        compile(tree, "library.py", "exec")  # Compilation only; never exec/import.
        imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        self.assertEqual(imports, {"argparse", "hashlib", "json", "socketserver", "sqlite3", "sys"})
        self.assertFalse(any(isinstance(node, ast.ImportFrom) for node in ast.walk(tree)))
        self.assertEqual(fixture.source_files(), {"library.py": fixture.MIXED_SOURCE.encode("utf-8")})
        strings = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        self.assertFalse(any("/api/" in value or "HTTP-PERSIST" in value or "doc-" in value for value in strings))
        wrapper = ast.parse(Path(fixture.__file__).read_text())
        calls = {node.func.id for node in ast.walk(wrapper) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        self.assertFalse(calls.intersection({"exec", "eval", "compile", "__import__"}))

    def test_expected_facts_are_fresh_and_sensitive_to_argv_or_wire(self):
        rows = fixture.expected_observations(self.recipe)
        rows[0]["json"]["after"]["events"] = 900
        self.assertEqual(fixture.expected_observations(self.recipe)[0]["json"]["after"]["events"], 1)
        cli = self.recipe.steps[40]
        original_event = fixture.expected_facts(self.recipe)[38]["json"]["event"]
        self.assertEqual(original_event["argv"], list(cli.argv))
        server = self.recipe.server_argv
        request = self.recipe.steps[1].request_json
        steps = (execution.HttpStep("start-one", "start", argv=server),
                 execution.HttpStep("probe-one", "probe", request), execution.HttpStep("stop-one", "stop"),
                 execution.HttpStep("cli-one", "cli", argv=cli.argv),
                 execution.HttpStep("start-two", "start", epoch=2, argv=server),
                 execution.HttpStep("probe-two", "probe", request, epoch=2),
                 execution.HttpStep("stop-two", "stop", epoch=2))
        compact = execution.HttpRecipe("mechanics-v3-declaration-sensitivity", server, (), (), steps,
            port=self.recipe.port, database_path=self.recipe.database_path)
        original = fixture.expected_facts(compact)
        changed_steps = (*steps[:3], replace(steps[3], argv=(*cli.argv[:-1], "different-fixture-command")), *steps[4:])
        changed = fixture.expected_facts(replace(compact, steps=changed_steps))
        self.assertEqual(original[0], changed[0])
        self.assertNotEqual(original[1]["json"]["after"]["chain"], changed[1]["json"]["after"]["chain"])
        self.assertNotEqual(original[2]["json"]["before"]["chain"], changed[2]["json"]["before"]["chain"])
        # Literal request digest must include header spelling/order and body.
        for row in fixture.expected_observations(self.recipe):
            if row["kind"] == "probe":
                step = self.recipe.steps[row["step_index"]]
                raw = wire.request_bytes(json.loads(step.request_json), self.recipe.port)
                self.assertEqual(row["json"]["event"]["request_sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(row["json"]["event"]["request_bytes"], len(raw))


class CandidateHttpMixedV3TranscriptTests(unittest.TestCase):
    HEAD = (b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n"
            b"Content-Type: application/vnd.docker.raw-stream\r\n\r\n")

    @staticmethod
    def frame(channel, payload):
        return bytes((channel, 0, 0, 0)) + len(payload).to_bytes(4, "big") + payload

    def test_independent_frame_parser_preserves_both_binary_channels(self):
        stdout, stderr = b"{\"fake\":true}\x00\xff", b"diagnostic\x00\x80"
        raw = self.HEAD + self.frame(1, stdout[:5]) + self.frame(2, stderr) + self.frame(1, stdout[5:])
        self.assertEqual(decode_attach_transcript(raw, stream_limit=100, frame_limit=100, eof_after_start=True),
                         {"stdout": stdout, "stderr": stderr, "frames": 3})

    def test_missing_eof_incomplete_frames_and_invalid_channels_are_rejected(self):
        frame = self.frame(1, b"abcd")
        for raw, eof in ((self.HEAD + frame, False), (self.HEAD + frame[:-1], True),
                         (self.HEAD + frame[:7], True), (self.HEAD + self.frame(0, b"x"), True),
                         (self.HEAD + bytes((1, 1, 0, 0)) + frame[4:], True)):
            with self.subTest(raw=raw, eof=eof), self.assertRaises(ValueError):
                decode_attach_transcript(raw, stream_limit=100, frame_limit=100, eof_after_start=eof)

    def test_caps_duplicate_upgrade_headers_and_nonupgrade_are_rejected(self):
        variants = (self.HEAD + self.frame(1, b"abcd"),
                    self.HEAD + self.frame(1, b"ab") + self.frame(1, b"cd"),
                    self.HEAD.replace(b"Connection: Upgrade", b"Connection: Upgrade\r\nConnection: Upgrade"),
                    self.HEAD.replace(b"101 UPGRADED", b"200 OK"),
                    self.HEAD[:-2] + b"".join(b"X-" + str(i).encode() + b": v\r\n" for i in range(125)) + b"\r\n")
        for raw in variants:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode_attach_transcript(raw, stream_limit=3, frame_limit=3, eof_after_start=True)
