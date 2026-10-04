"""Prospective real-Engine composition fault controls; no product acceptance.

Only this module's explicitly recorded creation-command/retention seams are
injected. Engine replies, attachment bytes, process exits and HTTP replies are
real. The offline class exercises guard/assertion logic using supplied values;
it cannot qualify any physical boundary. No production protocol is changed.
"""
from __future__ import annotations

from dataclasses import asdict, replace
import errno
import os
from pathlib import Path
import re
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_cases_v1 as cli_cases
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_http_execution_v3 as execution
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness.gitstore import GitStore
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from tests import candidate_engine_control_framing_v1 as control_framing
from tests import candidate_http_v3_mixed_fixture as fixtures
from tests import test_candidate_http_docker_v3 as raw_checks
from tests import test_candidate_http_mixed_v3 as mixed_checks

PURPOSE = "harness_qualification"
FAULT_PROTOCOL = "http-v3-composition-intentional-faults-v1"
PORT = 18765
DATABASE = "/tmp/catalog.sqlite"
CONTROL_ROSTER = (
    ("C01", "cli-wrong-volume", "step-003-cli-create"),
    ("C02", "cli-keeper-profile", "step-003-cli-create"),
    ("C03", "keeper-stopped-after-cli", "step-003-cli-process-result.json"),
    ("C04", "keeper-replaced-after-cli", "step-003-cli-process-result.json"),
    ("C05", "cli-observation-retention-failure", "step-003-cli-observation.json"),
)


def policy():
    return execution.HttpPolicy(RUNTIME_IMAGE, wire_limits=wire.WireLimits(
        response_limit_bytes=65536, timeout_seconds=5.0), lifetime_seconds=900)


def continuity_recipe():
    prefix = ("python", "-m", "library", "--db", DATABASE, "--root", "/inputs")
    server = (*prefix, "serve", "--port", str(PORT))
    request = execution.encoded({"method": "GET", "target": "/fixture/continuity",
        "headers": [["Host", "127.0.0.1:" + str(PORT)], ["Connection", "close"]],
        "body_b64": ""})
    return execution.HttpRecipe("http-v3-continuity", server, (), (), (
        execution.HttpStep("first-start", "start"),
        execution.HttpStep("prior-http", "probe", request),
        execution.HttpStep("first-stop", "stop"),
        execution.HttpStep("finite-cli", "cli", argv=(*prefix, "mechanics", "marker")),
        execution.HttpStep("dependent-start", "start"),
        execution.HttpStep("dependent-http", "probe", request),
        execution.HttpStep("dependent-stop", "stop")), port=PORT, database_path=DATABASE)


def control_definitions():
    recipe = continuity_recipe()
    return tuple({"control_id": identifier, "fault": fault, "injection_boundary": boundary,
        "recipe": recipe.record(), "policy": asdict(policy()), "purpose": PURPOSE,
        "expected": fixtures.expected_facts(recipe),
        "planned_counts": {"steps": 7, "requests": 2, "cli": 1, "servers": 2, "keepers": 1},
        "expected_observed_counts": {"entered_steps": 4, "completed_steps": 3,
            "probes": 1, "cli_attempts": int(identifier not in ("C01", "C02")),
            "servers": 1, "cli_containers": 1, "keeper_containers": 1},
        "fault_owned_extra_resources": {"volumes": int(identifier == "C01"),
            "keeper_containers": int(identifier == "C04")},
        "controller_cleanup_expected": identifier != "C04",
        "physical_result": "observation_unavailable", "acceptance_credit": False,
        "repeat_on_failure": False} for identifier, fault, boundary in CONTROL_ROSTER)


def mutated_cli_argv(spec, image_id, fault, *, alternate_volume=""):
    """Intentional bad creation request; expected controller spec stays unchanged."""
    if spec.role != "cli":
        raise ValueError("Fault may alter only the declared finite CLI creation")
    if fault == "cli-wrong-volume":
        if not alternate_volume or alternate_volume == spec.volume:
            raise ValueError("Distinct preowned alternate volume required")
        altered = replace(spec, volume=alternate_volume)
    elif fault == "cli-keeper-profile":
        # Keep the declared CLI identity labels, ID and argv. The actual mount /
        # working-directory profile belongs to the closed keeper role instead.
        # The container must remain created: the library module is not mounted.
        altered = replace(spec, role="keeper", binds=())
    else:
        raise ValueError("Unknown create-command fault")
    return execution.create_argv(altered, image_id), altered


def assert_exact_owned_container(test, actual, expected, image_id):
    """A name, similar labels or a running state never substitute for the full ID."""
    test.assertEqual(actual["Id"], expected["Id"])
    test.assertRegex(actual["Id"], r"^[0-9a-f]{64}$")
    test.assertEqual(actual["Name"], expected["Name"])
    test.assertEqual(actual["Image"], image_id)
    test.assertEqual(actual["Config"]["Labels"], expected["Config"]["Labels"])
    for field in ("Path", "Args", "Mounts"):
        test.assertEqual(raw_checks.encoded(actual[field]), raw_checks.encoded(expected[field]))


def assert_stopped_suffix(test, recipe, terminal, result, identifier):
    """Exact failed prefix; no dependent start/probe may disappear from census."""
    test.assertEqual(result.status, "observation_unavailable")
    test.assertEqual(terminal["entered_step_indices"], [0, 1, 2, 3])
    test.assertEqual(terminal["unentered_step_ids"], [step.step_id for step in recipe.steps[4:]])
    test.assertEqual(result.missing_step_ids, tuple(step.step_id for step in recipe.steps[3:]))
    test.assertEqual(len(terminal["steps"]), 3)
    test.assertEqual(terminal["attempted_probes"], 1)
    test.assertEqual(terminal["observed_probe_records"], 1)
    test.assertEqual(terminal["observed_requests"], 1)
    test.assertEqual(terminal["unknown_request_outcomes"], 0)
    test.assertEqual(terminal["attempted_cli"], int(identifier not in ("C01", "C02")))
    test.assertEqual(terminal["created_servers"], 1)
    test.assertEqual(terminal["created_helpers"], 1)
    test.assertEqual(terminal["created_keepers"], 1)
    test.assertEqual(terminal["created_cli"], 1)
    test.assertEqual(terminal["unconfirmed_creates"], 0)
    test.assertEqual(terminal["create_intents"], 4)
    test.assertEqual(len(result.observations), 1)
    test.assertIs(result.observations[0]["authenticated"], True)
    test.assertTrue(result.infrastructure)
    test.assertEqual(terminal["unavailable_cli_attempts"], int(identifier not in ("C01", "C02")))
    expected_cli_rows = int(identifier in ("C03", "C04"))
    test.assertEqual(len(result.cli_observations), expected_cli_rows)
    test.assertEqual(terminal["observed_cli_records"], expected_cli_rows)
    for row in result.cli_observations:
        test.assertIs(row["authenticated"], False)
    test.assertEqual(result.cleanup_verified, identifier != "C04")


def authenticate_failure_journal(test, journal, recipe, binding, result, checkpoints):
    """Authenticate fault-path journal without borrowing the success cleanup gate.

    All checkpoint bytes and exact prefix references are reconstructed here.
    Raw HTTP/CLI transport and each cleanup command are checked separately.
    """
    paths = sorted(journal.parent.glob("external-checkpoint-*.json"))
    test.assertEqual([path.name for path in paths],
        ["external-checkpoint-" + str(i).zfill(5) + ".json" for i in range(len(checkpoints))])
    test.assertTrue(paths)
    previous, first_seen = {}, {}
    for ordinal, (path, checkpoint) in enumerate(zip(paths, checkpoints)):
        value = raw_checks.read_journal(path)
        test.assertEqual(raw_checks.encoded(value), raw_checks.encoded(asdict(checkpoint)))
        test.assertEqual(set(value), {"files"})
        pairs = value["files"]
        test.assertIs(type(pairs), list)
        test.assertTrue(all(type(pair) is list and len(pair) == 2 for pair in pairs))
        for name, digest in pairs:
            test.assertRegex(name, r"^[a-z][a-z0-9.-]{0,180}$")
            test.assertRegex(digest, r"^[0-9a-f]{64}$")
        current = dict(pairs)
        test.assertEqual(len(current), len(pairs))
        test.assertEqual(pairs, [[name, digest] for name, digest in sorted(current.items())])
        test.assertEqual(len(current), len(previous) + 1)
        test.assertEqual({name: current[name] for name in previous}, previous)
        for name in current:
            first_seen.setdefault(name, ordinal)
        previous = current
    test.assertEqual(asdict(result.checkpoint), asdict(checkpoints[-1]))
    test.assertEqual(previous, {path.name: raw_checks.sha256(raw_checks.journal_reader.read(path))
        for path in journal.iterdir() if path.name != "owner.lock"})
    terminal = raw_checks.read_journal(journal / "terminal.json")
    config, intent = (raw_checks.read_journal(journal / name) for name in ("config.json", "intent.json"))
    test.assertEqual(result.terminal_sha256, raw_checks.sha256((journal / "terminal.json").read_bytes()))
    test.assertEqual(first_seen["terminal.json"], len(checkpoints) - 1)
    for record in (config, intent, terminal):
        test.assertEqual(record["protocol"], execution.PROTOCOL)
    test.assertEqual(config["mode"], "physical")
    test.assertEqual(terminal["mode"], "physical")
    test.assertEqual(config["root"], str(journal.resolve()))
    test.assertEqual(config["recipe"], recipe.record())
    test.assertEqual(raw_checks.encoded(config["registration"]["binding"]), raw_checks.encoded(asdict(binding)))
    test.assertEqual(binding.purpose, PURPOSE)
    test.assertEqual(intent["execution_id"], result.execution_id)
    test.assertEqual(intent["config_sha256"], raw_checks.sha256(raw_checks.encoded(config)))
    test.assertEqual(intent["registration_sha256"], raw_checks.sha256(raw_checks.encoded(config["registration"])))
    test.assertEqual(terminal["intent_sha256"], raw_checks.sha256((journal / "intent.json").read_bytes()))
    test.assertEqual(terminal["evaluator_sources_after"], config["evaluator_sources"])
    test.assertEqual(config["evaluator_sources"], execution.evaluator_sources())
    test.assertEqual(terminal["quota_policy"], execution.quota_policy())
    test.assertEqual(terminal["role_policy"], execution.role_policy_identity())
    test.assertEqual(terminal["definition_sha256"], recipe.definition_sha256)
    test.assertEqual(intent["planned_steps"], 7)
    for record in (intent, terminal):
        test.assertEqual(record["planned_requests"], 2)
        test.assertEqual(record["planned_cli"], 1)
    expected_paths = {"steps": ["step-000-result.json", "step-001-result.json", "step-002-result.json"],
        "observations": ["step-001-observation.json"], "epochs": ["step-000-epoch.json"],
        "cli_observations": ["step-003-cli-observation.json"] if result.cli_observations else []}
    decoded = {key: raw_checks.authenticated_descriptors(test, journal, terminal[key], names, previous, first_seen)
        for key, names in expected_paths.items()}
    for key in ("steps", "observations", "cli_observations"):
        for row in decoded[key]:
            index = row["step_index"]
            test.assertIs(type(index), int)
            step = recipe.steps[index]
            test.assertEqual(row["step_id"], step.step_id)
            test.assertEqual(row["kind"], step.kind)
            test.assertEqual(row["label"], "step-" + str(index).zfill(3))
            test.assertEqual(row["epoch"], step.epoch)
            test.assertEqual(row["root_path"], step.root_path)
            test.assertEqual(row["step_sha256"], raw_checks.sha256(raw_checks.encoded(step.record())))
            test.assertEqual(row["definition_sha256"], recipe.definition_sha256)
    test.assertEqual(decoded["observations"][0], decoded["steps"][1])
    test.assertEqual(decoded["observations"][0], {key: value for key, value in result.observations[0].items() if key != "wire"})
    test.assertEqual(decoded["cli_observations"], list(result.cli_observations))
    test.assertEqual(terminal["probe_transport_attempts"], [{"step_index": 1, "label": "step-001",
        "container_id": result.observations[0]["probe_id"], "artifact_prefix": "step-001-probe-",
        "meaning": "probe transport invoked; start/send not inferred"}])
    for attempt in terminal["cli_transport_attempts"]:
        test.assertEqual(attempt["step_index"], 3)
        test.assertEqual(attempt["label"], "step-003")
        test.assertEqual(attempt["artifact_prefix"], "step-003-cli-process-")
    return {**terminal, **decoded, "_authenticated_first_seen": first_seen}


class FaultBoundary:
    """Own only explicitly injected resources; keep every real control transcript."""
    def __init__(self, test, controller, root, identifier):
        self.test, self.controller, self.root = test, controller, root
        self.identifier = identifier
        self.row = next(row for row in control_definitions() if row["control_id"] == identifier)
        self.root.mkdir()
        self.original_command = controller._command
        self.original_retain = controller._retain
        self.hits = 0
        self.alternate_volume = None
        self.replacement = None
        self.original_keeper = None
        self.commands = []
        self.event = None
        self.cleanup_observations = []
        self.checkpoint_before_fault = None

    def command(self, label, arguments, *, clean=True):
        if label in self.commands:
            raise AssertionError("No retry/overwriting a fault control")
        self.commands.append(label)
        record = raw_checks.retained_docker_command(self.root, label, self.controller.endpoint, arguments)
        if clean:
            self.test.assertTrue(execution.CandidateHttpExecution._clean(record), record)
        return record

    def raw(self, record):
        return raw_checks.retained_bytes(self.test, self.root, record["stdout"])

    def inspect(self, label, identity):
        record = self.command(label, ["inspect", "--format", "{{json .}}", identity])
        value = engine.strict_json_loads(self.raw(record))
        self.test.assertIs(type(value), dict)
        return value

    def retained_event(self, value):
        if self.event is not None:
            raise AssertionError("Fault injection must occur exactly once")
        self.event = {"protocol": FAULT_PROTOCOL, "control_id": self.identifier,
            "fault": self.row["fault"], "injection_boundary": self.row["injection_boundary"],
            "purpose": PURPOSE, "intentional_fault": True,
            "journal_checkpoint_before_fault": asdict(self.checkpoint_before_fault), **value}
        raw_checks.write_new(self.root / "injected-fault.json", self.event)

    def create_alternate_volume(self):
        intent = raw_checks.read_journal(self.controller.root / "intent.json")
        name = intent["volume"] + "-fault"
        before = self.command("alternate-volume-before", ["volume", "ls", "--quiet", "--filter", "name=^" + name + "$"])
        self.test.assertEqual(self.raw(before), b"")
        labels = {"gossip.execution": intent["execution_id"], "gossip.qualification-fault": self.identifier}
        argv = ["volume", "create", "--driver", "local"]
        for key, value in sorted(labels.items()):
            argv += ["--label", key + "=" + value]
        for key, value in execution.VOLUME_OPTIONS.items():
            argv += ["--opt", key + "=" + value]
        argv.append(name)
        # The exact prior absence + retained create intent remain evidence even
        # if the process outcome is uncertain. Never delete by inferred name.
        record = self.command("alternate-volume-create", argv)
        self.test.assertEqual(self.raw(record).strip(), name.encode())
        inspected = self.command("alternate-volume-created", ["volume", "inspect", "--format", "{{json .}}", name])
        baseline = engine.strict_json_loads(self.raw(inspected))
        self.test.assertEqual(baseline["Name"], name)
        self.test.assertEqual(baseline["Options"], execution.VOLUME_OPTIONS)
        self.test.assertEqual(baseline["Labels"], labels)
        self.alternate_volume = baseline
        return name

    def dispatch_command(self, label, argv, timeout=None):
        if self.identifier in ("C01", "C02") and label == "step-003-cli-create":
            self.hits += 1
            self.test.assertEqual(self.hits, 1)
            self.checkpoint_before_fault = self.controller.checkpoint()
            declared = raw_checks.read_journal(self.controller.root / "step-003-cli-create-intent.json")
            spec = raw_checks.role_spec(declared["spec"])
            self.test.assertEqual(argv, declared["create_argv"])
            alternate = self.create_alternate_volume() if self.identifier == "C01" else ""
            altered_argv, altered_spec = mutated_cli_argv(spec, self.controller.policy.image_id,
                self.row["fault"], alternate_volume=alternate)
            self.retained_event({"expected_argv": argv, "executed_argv": altered_argv,
                "expected_spec": asdict(spec), "actual_spec": asdict(altered_spec),
                "actual_replies_are_unmodified": True})
            return self.original_command(label, altered_argv, timeout)
        return self.original_command(label, argv, timeout)

    def retain(self, name, raw):
        if self.identifier == "C05" and name == "step-003-cli-observation.json":
            self.hits += 1
            self.test.assertEqual(self.hits, 1)
            self.checkpoint_before_fault = self.controller.checkpoint()
            attempt_path = self.root / "controller-unretained-cli-observation.bin"
            with attempt_path.open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            self.retained_event({"retained_row": False, "failed_path": name,
                "attempted_observation": {"path": attempt_path.name, "bytes": len(raw),
                    "sha256": raw_checks.sha256(raw)},
                "attempted_row_is_controller_authenticated_observation": False,
                "attempted_bytes": len(raw), "attempted_sha256": raw_checks.sha256(raw),
                "error": "OSError:ENOSPC", "physical_disk_exhaustion_claim": False,
                "real_transport_result_retained_before_injection": "step-003-cli-process-result.json"})
            raise OSError(errno.ENOSPC, "intentional qualification observation retention fault")
        self.original_retain(name, raw)
        if self.identifier in ("C03", "C04") and name == "step-003-cli-process-result.json":
            self.hits += 1
            self.test.assertEqual(self.hits, 1)
            self.checkpoint_before_fault = self.controller.checkpoint()
            self.inject_keeper_fault()

    def inject_keeper_fault(self):
        baseline = raw_checks.read_journal(self.controller.root / "keeper-running.json")
        declared = raw_checks.read_journal(self.controller.root / "keeper-create-intent.json")
        spec = raw_checks.role_spec(declared["spec"])
        current = self.inspect("keeper-before-fault", baseline["Id"])
        assert_exact_owned_container(self.test, current, baseline, RUNTIME_IMAGE)
        execution.validate_role(current, spec, RUNTIME_IMAGE, self.controller.runtime)
        execution.validate_state(current, "running")
        self.original_keeper = baseline
        self.command("keeper-fault-stop", ["stop", "--time", "5", baseline["Id"]])
        stopped = self.inspect("keeper-after-stop", baseline["Id"])
        assert_exact_owned_container(self.test, stopped, baseline, RUNTIME_IMAGE)
        execution.validate_state(stopped, "exited")
        detail = {"original_keeper_id": baseline["Id"], "original_inspection": baseline,
            "before": current, "stopped": stopped, "actual_replies_are_unmodified": True}
        if self.identifier == "C04":
            self.command("original-keeper-remove", ["rm", baseline["Id"]])
            absent = self.command("original-keeper-absence", ["container", "ls", "--all", "--quiet", "--filter", "id=" + baseline["Id"]])
            self.test.assertEqual(self.raw(absent), b"")
            before = self.command("replacement-name-before", ["container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
            self.test.assertEqual(self.raw(before), b"")
            argv = execution.create_argv(spec, RUNTIME_IMAGE)
            created = self.command("replacement-create", argv[1:])
            identity = self.raw(created).strip().decode("ascii")
            self.test.assertRegex(identity, r"^[0-9a-f]{64}$")
            self.test.assertNotEqual(identity, baseline["Id"])
            replacement = self.inspect("replacement-created", identity)
            self.test.assertEqual(replacement["Id"], identity)
            execution.validate_role(replacement, spec, RUNTIME_IMAGE, self.controller.runtime)
            execution.validate_state(replacement, "created")
            self.replacement = replacement
            self.command("replacement-start", ["start", identity])
            running = self.inspect("replacement-running", identity)
            assert_exact_owned_container(self.test, running, replacement, RUNTIME_IMAGE)
            execution.validate_state(running, "running")
            detail.update(replacement_spec=asdict(spec), replacement_argv=argv,
                replacement_created=replacement, replacement_running=running,
                expected_guard="original full keeper ID remains authoritative; same-name replacement is foreign")
        self.retained_event(detail)

    def cleanup_fault_resources(self):
        """Only exact independently matched resources. No retries on failed cleanup."""
        if self.replacement is not None:
            value = self.inspect("replacement-cleanup-inspect", self.replacement["Id"])
            assert_exact_owned_container(self.test, value, self.replacement, RUNTIME_IMAGE)
            self.command("replacement-cleanup-remove", ["rm", "--force", self.replacement["Id"]])
            absent = self.command("replacement-cleanup-absence", ["container", "ls", "--all", "--quiet", "--filter", "id=" + self.replacement["Id"]])
            self.test.assertEqual(self.raw(absent), b"")
            self.cleanup_observations.append({"resource": "replacement-keeper", "id": self.replacement["Id"], "removed": True})
            # The controller deliberately refuses the same-name replacement and
            # retains its volume. Remove that exact original volume only after
            # every other registered container ID has independently disappeared.
            intent = raw_checks.read_journal(self.controller.root / "intent.json")
            for ordinal, path in enumerate(sorted(self.controller.root.glob("*-create-intent.json"))):
                created = raw_checks.read_journal(path.with_name(path.name.removesuffix("-intent.json") + ".json"))
                identity = raw_checks.retained_bytes(self.test, self.controller.root, created["stdout"]).strip().decode("ascii")
                absent = self.command("original-resource-absence-" + str(ordinal), ["container", "ls", "--all", "--quiet", "--filter", "id=" + identity])
                self.test.assertEqual(self.raw(absent), b"")
            baseline = raw_checks.read_journal(self.controller.root / "volume-baseline.json")
            self.remove_volume("original-volume", intent["volume"], baseline)
        if self.alternate_volume is not None:
            self.remove_volume("alternate-volume", self.alternate_volume["Name"], self.alternate_volume)
        raw_checks.write_new(self.root / "fault-owned-cleanup.json", {
            "control_id": self.identifier, "resources": self.cleanup_observations,
            "controller_cleanup_is_separate": True, "no_cleanup_retry": True})

    def remove_volume(self, label, name, baseline):
        inspected = self.command(label + "-cleanup-inspect", ["volume", "inspect", "--format", "{{json .}}", name])
        value = engine.strict_json_loads(self.raw(inspected))
        self.test.assertEqual(raw_checks.encoded(value), raw_checks.encoded(baseline))
        self.command(label + "-cleanup-remove", ["volume", "rm", name])
        absent = self.command(label + "-cleanup-absence", ["volume", "ls", "--quiet", "--filter", "name=^" + name + "$"])
        self.test.assertEqual(self.raw(absent), b"")
        self.cleanup_observations.append({"resource": label, "name": name,
            "baseline_sha256": raw_checks.sha256(raw_checks.encoded(baseline)), "removed": True})

    def seal(self):
        raw_checks.write_new(self.root / "fault-artifact-manifest.json", {
            "protocol": FAULT_PROTOCOL, "control_id": self.identifier, "fault_hits": self.hits,
            "ordered_commands": self.commands,
            "files": {path.name: {"bytes": path.stat().st_size, "sha256": raw_checks.sha256(path.read_bytes())}
                for path in sorted(self.root.iterdir()) if path.is_file()}})


def decode_missing_keeper_response(request, response, identity):
    """Exact missing-ID control with shared strict retained-byte framing only."""
    if type(identity) is not str or re.fullmatch(r"[0-9a-f]{64}", identity) is None:
        raise ValueError("Full original keeper ID required")
    expected = ("GET /v1.47/containers/" + identity + "/json HTTP/1.1\r\n"
        "Host: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode("ascii")
    if request != expected:
        raise ValueError("Exact original-ID inspection request required")
    decoded = control_framing.decode_control_frame(response)
    if decoded["status"] != 404 or decoded["framing_complete"] is not True:
        raise ValueError("Complete unambiguous framed 404 required")
    # An error file ending is not independent evidence that the socket reached
    # EOF. The shared parser keeps that field None for this retained-byte use.
    if decoded["socket_eof_observed"] is not None:
        raise ValueError("Unobserved socket EOF must remain unknown")
    value = engine.strict_json_loads(decoded["body"])
    if type(value) is not dict or type(value.get("message")) is not str or identity not in value["message"]:
        raise ValueError("404 must identify the missing original keeper")
    return value


def authenticate_fault_artifacts(test, driver, terminal):
    """Seal and independently reconstruct each actual injected command/result."""
    root = driver.root
    manifest = raw_checks.read_journal(root / "fault-artifact-manifest.json")
    test.assertEqual(manifest["protocol"], FAULT_PROTOCOL)
    test.assertEqual(manifest["control_id"], driver.identifier)
    test.assertEqual(manifest["fault_hits"], 1)
    expected = {path.name: {"bytes": path.stat().st_size, "sha256": raw_checks.sha256(path.read_bytes())}
        for path in sorted(root.iterdir()) if path.name != "fault-artifact-manifest.json"}
    test.assertEqual(manifest["files"], expected)
    test.assertEqual(manifest["ordered_commands"], driver.commands)
    test.assertEqual(len(driver.commands), len(set(driver.commands)))
    event = raw_checks.read_journal(root / "injected-fault.json")
    test.assertEqual(raw_checks.encoded(event), raw_checks.encoded(driver.event))
    test.assertEqual(event["protocol"], FAULT_PROTOCOL)
    test.assertEqual(event["purpose"], PURPOSE)
    test.assertIs(event["intentional_fault"], True)
    test.assertEqual(event["control_id"], driver.identifier)
    test.assertEqual(event["fault"], driver.row["fault"])
    test.assertEqual(event["injection_boundary"], driver.row["injection_boundary"])
    prefix = event["journal_checkpoint_before_fault"]["files"]
    test.assertEqual(raw_checks.encoded(prefix), raw_checks.encoded(asdict(driver.checkpoint_before_fault)["files"]))
    journal_files = dict(terminal["_authenticated_first_seen"])
    test.assertEqual({name for name, _ in prefix},
        {name for name, ordinal in journal_files.items() if ordinal < len(prefix)})
    for name, digest in prefix:
        test.assertEqual(raw_checks.sha256((driver.controller.root / name).read_bytes()), digest)
    target = "step-003-cli-create-intent.json" if driver.identifier in ("C01", "C02") else "step-003-cli-process-result.json"
    test.assertIn(target, dict(prefix))
    test.assertNotIn("step-004-create-intent.json", dict(prefix))
    endpoint = ["docker", "--host", "unix://" + driver.controller.endpoint.socket_path]
    for label in driver.commands:
        intent = raw_checks.read_journal(root / (label + "-intent.json"))
        result = raw_checks.read_journal(root / (label + ".json"))
        test.assertEqual(intent["purpose"], PURPOSE)
        test.assertEqual(intent["argv"], result["argv"])
        test.assertEqual(result["argv"][:3], endpoint)
        test.assertTrue(execution.CandidateHttpExecution._clean(result), result)
        for kind in ("stdout", "stderr"):
            raw = raw_checks.retained_bytes(test, root, result[kind])
            test.assertEqual(result[kind]["path"], label + "-" + kind + ".bin")
            test.assertEqual(result[kind]["observed_bytes"], len(raw))
    cleanup = raw_checks.read_journal(root / "fault-owned-cleanup.json")
    test.assertEqual(cleanup["resources"], driver.cleanup_observations)
    test.assertIs(cleanup["controller_cleanup_is_separate"], True)
    test.assertIs(cleanup["no_cleanup_retry"], True)


def assert_c05_attempt_eligible(test, attempted):
    """A second real continuity failure cannot qualify the retention control.

    This only rejects unsupported attempted-row claims. Physical qualification
    additionally reconstructs every relevant raw lineage and retirement record.
    It never adds this row to the controller's authenticated observations.
    """
    test.assertEqual((attempted["kind"], attempted["step_index"]), ("cli", 3))
    test.assertIs(attempted["authenticated"], True)
    test.assertIs(attempted["cli_removed"], True)
    test.assertNotIn("observation_error", attempted)
    test.assertNotIn("cleanup_error", attempted)
    process = attempted["process"]
    test.assertEqual(process["status"], "completed")
    test.assertIs(process["completion"]["natural"], True)
    test.assertIs(process["capture_complete"], True)


def assert_c05_retention_boundary(test, journal, recipe, process, terminal, driver):
    """Prove successful post-CLI lineage and retirement preceded the sole fault."""
    test.assertEqual(driver.identifier, "C05")
    descriptor = driver.event["attempted_observation"]
    test.assertEqual(set(descriptor), {"path", "bytes", "sha256"})
    test.assertEqual(descriptor["path"], "controller-unretained-cli-observation.bin")
    raw = raw_checks.retained_bytes(test, driver.root, descriptor)
    attempted = raw_checks.strict_control_json(raw)
    test.assertEqual(raw, raw_checks.encoded(attempted))
    test.assertEqual(driver.event["attempted_bytes"], len(raw))
    test.assertEqual(driver.event["attempted_sha256"], raw_checks.sha256(raw))
    test.assertIs(driver.event["retained_row"], False)
    test.assertIs(driver.event["attempted_row_is_controller_authenticated_observation"], False)
    assert_c05_attempt_eligible(test, attempted)
    test.assertEqual(attempted["process"], process)
    test.assertEqual(terminal["cli_observations"], [])
    test.assertFalse((journal / "step-003-cli-observation.json").exists())
    test.assertFalse((journal / descriptor["path"]).exists())
    config = raw_checks.read_journal(journal / "config.json")
    intent = raw_checks.read_journal(journal / "intent.json")
    step, label = recipe.steps[3], "step-003"
    binding = config["registration"]["binding"]
    test.assertEqual(attempted["binding"], binding)
    test.assertEqual(attempted["step_id"], step.step_id)
    test.assertEqual(attempted["label"], label)
    test.assertEqual(attempted["epoch"], step.epoch)
    test.assertEqual(attempted["root_path"], step.root_path)
    test.assertEqual(attempted["expected_argv"], list(step.argv))
    test.assertEqual(attempted["database_path"], recipe.database_path)
    test.assertEqual(attempted["database_volume"], intent["volume"])
    test.assertEqual(attempted["source_sha256"], binding["source_sha256"])
    test.assertEqual(attempted["fixture_sha256"], binding["fixture_sha256"])
    test.assertEqual(attempted["step_sha256"], raw_checks.sha256(raw_checks.encoded(step.record())))
    test.assertEqual(attempted["definition_sha256"], recipe.definition_sha256)
    test.assertEqual(attempted["product_verdict"], "not_evaluated")
    first_seen = terminal["_authenticated_first_seen"]
    keeper = raw_checks.read_journal(journal / "keeper-running.json")
    keeper_spec = raw_checks.role_spec(raw_checks.read_journal(journal / "keeper-create-intent.json")["spec"])
    test.assertEqual(attempted["keeper_id"], keeper["Id"])
    for suffix in ("keeper-before", "cli-keeper-prestart", "cli-keeper-after"):
        stem = label + "-" + suffix
        test.assertEqual((journal / (stem + "-request.bin")).read_bytes(),
            ("GET /v1.47/containers/" + keeper["Id"] + "/json HTTP/1.1\r\n"
             "Host: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode())
        current = raw_checks.retained_engine_json(test, journal, stem)
        execution.validate_role(current, keeper_spec, RUNTIME_IMAGE, test.runtime)
        execution.validate_state(current, "running")
        comparison = raw_checks.read_journal(journal / (stem + "-continuity.json"))
        test.assert_running_interval(keeper, current, comparison)
        if suffix == "cli-keeper-after":
            test.assertEqual(comparison, attempted["keeper_continuity"])
    volume = raw_checks.read_journal(journal / "volume-baseline.json")
    test.assertEqual(volume["Name"], intent["volume"])
    test.assertEqual(volume["Driver"], "local")
    test.assertEqual(volume["Options"], execution.VOLUME_OPTIONS)
    test.assertEqual(volume["Labels"], {"gossip.execution": intent["execution_id"],
        "gossip.snapshot": execution.SNAPSHOT_PROTOCOL})
    for stem in ("volume-created", label + "-cli-volume-before", label + "-cli-volume-after"):
        record = raw_checks.read_journal(journal / (stem + ".json"))
        test.assertEqual(record["argv"], ["docker", "--host", "unix://" + test.runtime["endpoint"]["socket_path"],
            "volume", "inspect", "--format", "{{json .}}", volume["Name"]])
        test.assertTrue(execution.CandidateHttpExecution._clean(record), record)
        for kind in ("stdout", "stderr"):
            data = raw_checks.retained_bytes(test, journal, record[kind])
            test.assertEqual(record[kind]["path"], stem + "-" + kind + ".bin")
            test.assertEqual(record[kind]["observed_bytes"], len(data))
        test.assertEqual(raw_checks.retained_bytes(test, journal, record["stderr"]), b"")
        value = raw_checks.strict_control_json(raw_checks.retained_bytes(test, journal, record["stdout"]))
        test.assertEqual(raw_checks.encoded(value), raw_checks.encoded(volume))
    # The existing independent cleanup assertion has reconstructed exact CLI
    # ID/labels, retirement intent/remove command and empty inventory already.
    created = raw_checks.retained_engine_json(test, journal, label + "-cli-created")
    retirement = raw_checks.read_journal(journal / (label + "-cli-retire-intent.json"))
    test.assertEqual(attempted["cli_id"], created["Id"])
    test.assertEqual(retirement["container_id"], created["Id"])
    inspected = raw_checks.read_journal(journal / (label + "-cli-retire-inspect.json"))
    retired_state = raw_checks.strict_control_json(raw_checks.retained_bytes(test, journal, inspected["stdout"]))
    execution.validate_state(retired_state, "exited")
    test.assertEqual(retired_state["State"]["ExitCode"], 0)
    test.assertEqual(retired_state["Id"], created["Id"])
    order = ["step-002-retire-absence.json", label + "-cli-create-intent.json",
        label + "-cli-keeper-prestart-response.bin", label + "-cli-volume-before.json",
        label + "-cli-process-intent.json", label + "-cli-process-result.json",
        label + "-cli-keeper-after-response.bin", label + "-cli-keeper-after-continuity.json",
        label + "-cli-volume-after.json", label + "-cli-retire-inspect.json",
        label + "-cli-retire-intent.json", label + "-cli-retire-remove.json",
        label + "-cli-retire-absence.json"]
    fault_prefix = dict(driver.event["journal_checkpoint_before_fault"]["files"])
    for name in order:
        test.assertIn(name, fault_prefix)
    for before, after in zip(order, order[1:]):
        test.assertLess(first_seen[before], first_seen[after], (before, after))
    test.assertEqual(first_seen[order[-1]], len(fault_prefix) - 1)
    test.assertLess(first_seen[order[-1]], first_seen["keeper-stop-before-request.bin"])


def assert_fault_cleanup(test, journal, terminal, driver):
    """Reconstruct successful owned removals and the expected replacement refusal."""
    config = raw_checks.read_journal(journal / "config.json")
    first_seen = terminal["_authenticated_first_seen"]
    endpoint = ["docker", "--host", "unix://" + config["endpoint"]["socket_path"]]

    def command(label, arguments):
        record = raw_checks.read_journal(journal / (label + ".json"))
        test.assertEqual(record["argv"], [*endpoint, *arguments])
        test.assertTrue(execution.CandidateHttpExecution._clean(record), record)
        for kind in ("stdout", "stderr"):
            raw = raw_checks.retained_bytes(test, journal, record[kind])
            test.assertEqual(record[kind]["observed_bytes"], len(raw))
        return raw_checks.retained_bytes(test, journal, record["stdout"])

    creates = []
    removals = {}
    for path in sorted(journal.glob("*-intent.json")):
        record = raw_checks.read_journal(path)
        if path.name.endswith("-create-intent.json"):
            creates.append((path.name.removesuffix("-create-intent.json"), record))
        elif {"container_id", "spec", "force", "inspection_sha256"} <= set(record):
            name = record["spec"]["name"]
            test.assertNotIn(name, removals)
            removals[name] = (path.name.removesuffix("-intent.json"), record)
    test.assertEqual(len(creates), 4)
    test.assertEqual(set(terminal["cleanup"]), {row["spec"]["name"] for _, row in creates})
    keeper_name = raw_checks.read_journal(journal / "intent.json")["keeper"]
    expected_removals = set(terminal["cleanup"])
    if driver.identifier == "C04":
        expected_removals.remove(keeper_name)
        test.assertIs(terminal["cleanup"][keeper_name], False)
        test.assertIs(terminal["volume_cleanup"], False)
        test.assertFalse((journal / "volume-remove.json").exists())
        test.assertFalse((journal / "volume-cleanup-inspect.json").exists())
        refusal = [raw_checks.read_journal(path) for path in journal.glob("cleanup-*-inspect.json")]
        matches = [row for row in refusal if engine.strict_json_loads(
            raw_checks.retained_bytes(test, journal, row["stdout"])).get("Name") == "/" + keeper_name]
        test.assertEqual(len(matches), 1)
        replacement = engine.strict_json_loads(raw_checks.retained_bytes(test, journal, matches[0]["stdout"]))
        assert_exact_owned_container(test, replacement, driver.replacement, RUNTIME_IMAGE)
        test.assertNotEqual(replacement["Id"], driver.original_keeper["Id"])
        test.assertTrue(any("Cleanup ownership differs" in value for value in terminal["infrastructure"]))
        test.assertEqual({row["resource"] for row in driver.cleanup_observations}, {"replacement-keeper", "original-volume"})
    else:
        test.assertIs(terminal["volume_cleanup"], True)
        test.assertTrue(all(value is True for value in terminal["cleanup"].values()))
    test.assertEqual(set(removals), expected_removals)
    for label, creation in creates:
        spec = creation["spec"]
        actual = creation["create_argv"]
        if label == "step-003-cli" and driver.identifier in ("C01", "C02"):
            test.assertEqual(actual, driver.event["expected_argv"])
            actual = driver.event["executed_argv"]
        identity = command(label + "-create", actual[1:]).strip().decode("ascii")
        test.assertRegex(identity, r"^[0-9a-f]{64}$")
        if spec["name"] not in removals:
            test.assertEqual(driver.identifier, "C04")
            test.assertEqual(identity, driver.original_keeper["Id"])
            continue
        removal_label, removal = removals[spec["name"]]
        test.assertEqual(raw_checks.encoded(removal["spec"]), raw_checks.encoded(spec))
        test.assertEqual(removal["container_id"], identity)
        value = engine.strict_json_loads(command(removal_label + "-inspect", ["inspect", "--format", "{{json .}}", spec["name"]]))
        test.assertEqual(value["Id"], identity)
        test.assertEqual(value["Name"], "/" + spec["name"])
        test.assertEqual(value["Image"], RUNTIME_IMAGE)
        test.assertEqual(value["Config"]["Labels"], dict(spec["labels"]))
        if label == "step-003-cli" and driver.identifier in ("C01", "C02"):
            execution.validate_state(value, "created")
        test.assertEqual(removal["inspection_sha256"], raw_checks.sha256(raw_checks.encoded(value)))
        command(removal_label + "-remove", ["rm", *(["--force"] if removal["force"] else []), identity])
        test.assertEqual(command(removal_label + "-absence", ["container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec["name"] + "$"]), b"")
        test.assertLess(first_seen[label + "-create-intent.json"], first_seen[removal_label + "-intent.json"])
        test.assertLess(first_seen[removal_label + "-intent.json"], first_seen[removal_label + "-remove.json"])
        test.assertLess(first_seen[removal_label + "-remove.json"], first_seen[removal_label + "-absence.json"])
    if driver.identifier != "C04":
        volume = raw_checks.read_journal(journal / "intent.json")["volume"]
        baseline = raw_checks.read_journal(journal / "volume-baseline.json")
        inspected = engine.strict_json_loads(command("volume-cleanup-inspect", ["volume", "inspect", "--format", "{{json .}}", volume]))
        test.assertEqual(raw_checks.encoded(inspected), raw_checks.encoded(baseline))
        command("volume-remove", ["volume", "rm", volume])
        test.assertEqual(command("volume-absence", ["volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"]), b"")
        test.assertLess(first_seen["volume-remove.json"], first_seen["volume-absence.json"])
        test.assertLess(first_seen["volume-absence.json"], first_seen["terminal.json"])


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit physical Docker lane required")
class CandidateHttpContinuityV3DockerTests(raw_checks.HttpV3PhysicalAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-c03-http-continuity-v3", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.controls = {row["control_id"]: row for row in control_definitions()}
        cls.outcomes = {key: {"control_id": key, "status": "not-run", "acceptance_credit": False}
            for key in cls.controls}
        cls.addClassCleanup(cls.save_census)
        cls.endpoint = engine.EngineEndpoint.from_environment()

        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())

        cls.runtime = engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=retain_runtime, label="class-runtime")
        cls.files = fixtures.source_files()
        cls.store = GitStore.create(cls.artifacts.root / "fixture.git",
            {name: raw.decode("utf-8") for name, raw in cls.files.items()})
        cls.commit = cls.store.head()
        cls.tree, captured = execution.capture_git_source(cls.store, cls.commit)
        if captured != cls.files:
            raise AssertionError("Committed inert fixture differs")
        cls.requirements_sha256 = cli_cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
        cls.roster_sha256 = raw_checks.sha256(raw_checks.encoded(list(cls.controls.values())))
        raw_checks.write_new(cls.artifacts.root / "prospective-controls.json", {
            "protocol": FAULT_PROTOCOL, "ordered_controls": list(cls.controls.values()),
            "ordered_roster_sha256": cls.roster_sha256, "source_commit": cls.commit,
            "source_tree": cls.tree, "source_files": {name: raw_checks.sha256(raw) for name, raw in cls.files.items()},
            "runtime": cls.runtime, "evaluator_sources": execution.evaluator_sources(),
            "assertion_sources": {Path(module.__file__).name: raw_checks.sha256(Path(module.__file__).read_bytes())
                for module in (fixtures, raw_checks, mixed_checks, control_framing)},
            "control_source_sha256": raw_checks.sha256(Path(__file__).read_bytes()),
            "purpose": PURPOSE, "history_count": 5, "complete_v3_gate": False,
            "physical_faults_are_intentional": True, "acceptance_credit": False,
            "no_model_calls": True, "no_retry_until_green": True})

    @classmethod
    def save_census(cls):
        raw_checks.write_new(cls.artifacts.root / "completion-census.json", {
            "planned": list(cls.controls), "outcomes": list(cls.outcomes.values()),
            "not_run": [key for key, value in cls.outcomes.items() if value["status"] == "not-run"],
            "not_qualified": [key for key, value in cls.outcomes.items() if value["status"] != "qualified"],
            "complete_v3_gate": False, "acceptance_credit": False,
            "production_acceptance_authority": False})

    def run_fault(self, identifier):
        self.assertEqual(self.outcomes[identifier]["status"], "not-run", "No automatic retry")
        self.outcomes[identifier]["status"] = "started-not-qualified"
        root = self.artifacts.root / identifier.lower()
        root.mkdir()
        recipe, bounds = continuity_recipe(), policy()
        binding = execution.binding_for(self.files, recipe, bounds, self.runtime,
            requirements_sha256=self.requirements_sha256)
        registration = execution.HttpRegistration(binding, self.commit, self.tree,
            "physical-http-continuity-v3-1")
        raw_checks.write_new(root / "prospective-registration.json", {
            "registration": asdict(registration), "definition": self.controls[identifier],
            "ordered_roster_sha256": self.roster_sha256, "purpose": PURPOSE})
        checkpoints = []

        def checkpoint_sink(checkpoint):
            raw_checks.write_new(root / ("external-checkpoint-" + str(len(checkpoints)).zfill(5) + ".json"), asdict(checkpoint))
            checkpoints.append(checkpoint)

        journal = root / "execution"
        with execution.CandidateHttpExecution(journal, self.store, registration, recipe, bounds,
                endpoint=self.endpoint, checkpoint_sink=checkpoint_sink) as controller:
            driver = FaultBoundary(self, controller, root / "intentional-fault", identifier)
            try:
                with patch.object(controller, "_command", driver.dispatch_command), patch.object(controller, "_retain", driver.retain):
                    result = controller.execute_once()
                self.assertEqual(result.checkpoint, controller.checkpoint())
            finally:
                # Controller owns normal resources; fault-owned exact IDs are
                # separately cleaned once, including on assertion/transport failure.
                try:
                    driver.cleanup_fault_resources()
                finally:
                    driver.seal()
        self.outcomes[identifier].update(execution_status=result.status,
            infrastructure=list(result.infrastructure), missing_step_ids=list(result.missing_step_ids),
            controller_cleanup_verified=result.cleanup_verified, terminal_sha256=result.terminal_sha256,
            intentional_fault_hits=driver.hits, fault_owned_cleanup=driver.cleanup_observations)
        raw_checks.write_new(root / "physical-census.json", self.outcomes[identifier])
        self.assertEqual(driver.hits, 1)
        self.assertIsNotNone(driver.event)
        terminal = authenticate_failure_journal(self, journal, recipe, binding, result, checkpoints)
        if not hasattr(self, "_authenticated_terminals"):
            self._authenticated_terminals = {}
        self._authenticated_terminals[journal] = terminal
        assert_stopped_suffix(self, recipe, terminal, result, identifier)
        assert_fault_cleanup(self, journal, terminal, driver)
        authenticate_fault_artifacts(self, driver, terminal)
        observed = self.authenticated_wire(journal, recipe, bounds, result.observations[0])
        self.assert_listener(observed, port=recipe.port)
        self.assertTrue(observed.exchange_complete)
        expected = {row["step_index"]: row["json"] for row in fixtures.expected_facts(recipe)}
        self.assertEqual(observed.response.status_code, 200)
        # The mixed fixture's facts are mechanics-only, independently declared.
        self.assertEqual(engine.strict_json_loads(observed.response.body), expected[1])
        self.assertFalse((journal / "step-004-create-intent.json").exists())
        self.assertFalse((journal / "step-005-probe-create-intent.json").exists())
        if identifier in ("C01", "C02"):
            self.assertFalse((journal / "step-003-cli-process-intent.json").exists())
            self.assertFalse((journal / "step-003-cli-process-start-request.bin").exists())
            actual = raw_checks.retained_engine_json(self, journal, "step-003-cli-created")
            actual_spec = raw_checks.role_spec(driver.event["actual_spec"])
            expected_spec = raw_checks.role_spec(driver.event["expected_spec"])
            execution.validate_role(actual, actual_spec, RUNTIME_IMAGE, self.runtime)
            execution.validate_state(actual, "created")
            with self.assertRaises(ValueError):
                execution.validate_role(actual, expected_spec, RUNTIME_IMAGE, self.runtime)
            self.assertEqual(actual["State"]["Pid"], 0)
            if identifier == "C01":
                mount = next(row for row in actual["Mounts"] if row["Destination"] == "/tmp")
                self.assertEqual(mount["Name"], driver.alternate_volume["Name"])
                self.assertNotEqual(mount["Name"], expected_spec.volume)
                self.assertTrue(mount["RW"])
                self.assertTrue(any("Owned DB volume role differs" in item for item in result.infrastructure))
            else:
                self.assertEqual({row["Destination"] for row in actual["Mounts"]}, {"/tmp"})
                self.assertFalse(actual["Mounts"][0]["RW"])
                self.assertEqual(actual["Config"]["WorkingDir"], "/")
                self.assertTrue(any("Role process restrictions differ" in item for item in result.infrastructure))
        else:
            process = raw_checks.read_journal(journal / "step-003-cli-process-result.json")
            streams = mixed_checks.assert_cli_process(self, journal, recipe, bounds, process, 3,
                first_seen=terminal["_authenticated_first_seen"])
            self.assertEqual(streams["stderr"], b"")
            self.assertEqual(engine.strict_json_loads(streams["stdout"]), expected[3])
            if identifier == "C05":
                assert_c05_retention_boundary(self, journal, recipe, process, terminal, driver)
                self.assertFalse((journal / "step-003-cli-observation.json").exists())
                self.assertEqual(terminal["cli_observations"], [])
                self.assertTrue(any("intentional qualification observation retention fault" in value for value in result.infrastructure))
            else:
                row = result.cli_observations[0]
                self.assertEqual(row["process"], process)
                self.assertIs(row["authenticated"], False)
                self.assertIs(row["cli_removed"], True)
                if identifier == "C03":
                    after = raw_checks.retained_engine_json(self, journal, "step-003-cli-keeper-after")
                    self.assertEqual(after["Id"], driver.original_keeper["Id"])
                    execution.validate_state(after, "exited")
                    self.assertIn("Role lifecycle state differs", row["observation_error"])
                else:
                    self.assertNotEqual(driver.replacement["Id"], driver.original_keeper["Id"])
                    self.assertEqual(driver.replacement["Name"], driver.original_keeper["Name"])
                    request = (journal / "step-003-cli-keeper-after-request.bin").read_bytes()
                    response = (journal / "step-003-cli-keeper-after-response.bin").read_bytes()
                    decode_missing_keeper_response(request, response, driver.original_keeper["Id"])
                    self.assertIn("Engine control response failed", row["observation_error"])
        self.outcomes[identifier].update(status="qualified", qualification_verified=True,
            preserved_prior_http=True, dependent_dispatch_prevented=True,
            acceptance_credit=False, full_requirement_verdict=None)
        raw_checks.write_new(root / "mechanics-outcome.json", self.outcomes[identifier])

    def test_c01_wrong_cli_volume_rejected_before_finite_dispatch(self):
        self.run_fault("C01")

    def test_c02_wrong_cli_role_profile_rejected_before_finite_dispatch(self):
        self.run_fault("C02")

    def test_c03_real_keeper_loss_after_real_cli_blocks_successor(self):
        self.run_fault("C03")

    def test_c04_same_name_keeper_replacement_does_not_gain_ownership(self):
        self.run_fault("C04")

    def test_c05_retention_failure_after_real_cli_preserves_prior_facts(self):
        self.run_fault("C05")


class CandidateHttpContinuityV3DefinitionTests(unittest.TestCase):
    """Supplied values only. These tests never qualify physical observations."""
    def spec(self):
        return execution.RoleSpec("cli", "test-cli", ("python", "/workspace/library.py", "--db", DATABASE,
            "--root", "/inputs", "mechanics", "marker"), (("gossip.role", "cli"),),
            (("/workspace", str(Path("/tmp/fixture-workspace").resolve())),
             ("/inputs", str(Path("/tmp/fixture-inputs").resolve()))), "test-volume")

    def test_exact_five_boundary_roster_and_unshortened_dependent_suffix(self):
        rows = control_definitions()
        self.assertEqual([row["control_id"] for row in rows], ["C01", "C02", "C03", "C04", "C05"])
        self.assertEqual(len({row["fault"] for row in rows}), 5)
        recipe = continuity_recipe()
        self.assertEqual([step.kind for step in recipe.steps], ["start", "probe", "stop", "cli", "start", "probe", "stop"])
        self.assertEqual([step.epoch for step in recipe.steps], [1, 1, 1, 1, 2, 2, 2])
        for row in rows:
            self.assertEqual(row["purpose"], PURPOSE)
            self.assertFalse(row["acceptance_credit"])
            self.assertFalse(row["repeat_on_failure"])
            self.assertEqual(row["recipe"], recipe.record())
            self.assertEqual(row["physical_result"], "observation_unavailable")

    def test_wrong_volume_changes_only_declared_volume_source(self):
        spec = self.spec()
        argv, actual = mutated_cli_argv(spec, RUNTIME_IMAGE, "cli-wrong-volume", alternate_volume="test-volume-fault")
        self.assertEqual(actual, replace(spec, volume="test-volume-fault"))
        expected = execution.create_argv(spec, RUNTIME_IMAGE)
        self.assertEqual(len(argv), len(expected))
        changes = [(left, right) for left, right in zip(expected, argv) if left != right]
        self.assertEqual(changes, [("type=volume,source=test-volume,target=/tmp,volume-nocopy",
            "type=volume,source=test-volume-fault,target=/tmp,volume-nocopy")])

    def test_wrong_role_is_real_source_free_readonly_keeper_profile(self):
        spec = self.spec()
        argv, actual = mutated_cli_argv(spec, RUNTIME_IMAGE, "cli-keeper-profile")
        self.assertEqual(actual.labels, spec.labels)
        self.assertEqual(actual.argv, spec.argv)
        self.assertEqual(actual.volume, spec.volume)
        self.assertEqual(actual.role, "keeper")
        self.assertEqual(actual.binds, ())
        self.assertIn("--workdir=/", argv)
        self.assertIn("type=volume,source=test-volume,target=/tmp,volume-nocopy,readonly", argv)
        self.assertFalse(any("type=bind," in argument for argument in argv))

    def test_create_mutation_refuses_other_roles_unknown_fault_or_same_volume(self):
        for fault, alternate, spec in (("unknown", "", self.spec()),
                ("cli-wrong-volume", "test-volume", self.spec()),
                ("cli-wrong-volume", "", self.spec()),
                ("cli-keeper-profile", "", replace(self.spec(), role="server"))):
            with self.subTest(fault=fault, alternate=alternate, role=spec.role), self.assertRaises(ValueError):
                mutated_cli_argv(spec, RUNTIME_IMAGE, fault, alternate_volume=alternate)

    def test_owned_cleanup_rejects_same_name_foreign_full_id(self):
        original = {"Id": "a" * 64, "Name": "/same-name", "Image": RUNTIME_IMAGE,
            "Config": {"Labels": {"gossip.role": "keeper"}}, "Path": "python", "Args": ["-I"], "Mounts": []}
        assert_exact_owned_container(self, original, original, RUNTIME_IMAGE)
        foreign = {**original, "Id": "b" * 64}
        with self.assertRaises(AssertionError):
            assert_exact_owned_container(self, foreign, original, RUNTIME_IMAGE)

    def test_owned_cleanup_rejects_changed_labels_image_argv_or_mounts(self):
        original = {"Id": "a" * 64, "Name": "/same-name", "Image": RUNTIME_IMAGE,
            "Config": {"Labels": {"gossip.role": "keeper"}}, "Path": "python", "Args": ["-I"], "Mounts": []}
        for key, value in (("Config", {"Labels": {"gossip.role": "server"}}), ("Image", "foreign"),
                ("Args", ["-c", "other"]), ("Mounts", [{"Name": "foreign"}])):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                assert_exact_owned_container(self, {**original, key: value}, original, RUNTIME_IMAGE)


    def supplied_failed_prefix(self, identifier):
        # Supplied assertion inputs only: no Engine source, bytes or timing is
        # invented or admitted as physical evidence by these negative checks.
        recipe = continuity_recipe()
        cli_rows = ({"authenticated": False},) if identifier in ("C03", "C04") else ()
        result = SimpleNamespace(status="observation_unavailable",
            missing_step_ids=tuple(step.step_id for step in recipe.steps[3:]),
            observations=({"authenticated": True},), cli_observations=cli_rows,
            infrastructure=("supplied-only",), cleanup_verified=identifier != "C04")
        terminal = {"entered_step_indices": [0, 1, 2, 3],
            "unentered_step_ids": [step.step_id for step in recipe.steps[4:]],
            "steps": [None] * 3, "attempted_probes": 1, "observed_probe_records": 1,
            "observed_requests": 1, "unknown_request_outcomes": 0,
            "attempted_cli": int(identifier not in ("C01", "C02")),
            "created_servers": 1, "created_helpers": 1, "created_keepers": 1,
            "created_cli": 1, "unconfirmed_creates": 0, "create_intents": 4,
            "unavailable_cli_attempts": int(identifier not in ("C01", "C02")),
            "observed_cli_records": len(cli_rows)}
        return recipe, terminal, result

    def test_failed_prefix_rejects_hidden_dependent_dispatch_or_lost_suffix(self):
        recipe, terminal, result = self.supplied_failed_prefix("C03")
        assert_stopped_suffix(self, recipe, terminal, result, "C03")
        for key, value in (("entered_step_indices", [0, 1, 2, 3, 4]),
                ("unentered_step_ids", ["dependent-stop"]), ("created_servers", 2),
                ("observed_requests", 2), ("unknown_request_outcomes", 1)):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                assert_stopped_suffix(self, recipe, {**terminal, key: value}, result, "C03")

    def test_retained_cli_bytes_cannot_be_counted_as_authenticated_observation(self):
        recipe, terminal, result = self.supplied_failed_prefix("C05")
        assert_stopped_suffix(self, recipe, terminal, result, "C05")
        for key, value in (("unavailable_cli_attempts", 0), ("observed_cli_records", 1), ("attempted_cli", 0)):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                assert_stopped_suffix(self, recipe, {**terminal, key: value}, result, "C05")
        result.cli_observations = ({"authenticated": True},)
        with self.assertRaises(AssertionError):
            assert_stopped_suffix(self, recipe, terminal, result, "C05")

    def test_replacement_cleanup_does_not_erase_controller_ownership_refusal(self):
        recipe, terminal, result = self.supplied_failed_prefix("C04")
        assert_stopped_suffix(self, recipe, terminal, result, "C04")
        result.cleanup_verified = True
        with self.assertRaises(AssertionError):
            assert_stopped_suffix(self, recipe, terminal, result, "C04")


    def test_missing_keeper_requires_exact_original_id_and_complete_error_frame(self):
        identity = "a" * 64
        request = ("GET /v1.47/containers/" + identity + "/json HTTP/1.1\r\n"
            "Host: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode()
        body = raw_checks.encoded({"message": "No such container: " + identity})
        response = b"HTTP/1.1 404 Not Found\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
        self.assertEqual(decode_missing_keeper_response(request, response, identity), engine.strict_json_loads(body))
        wrong_request = request.replace(identity.encode(), ("b" * 64).encode())
        for supplied_request, supplied_response in ((wrong_request, response), (request, response[:-1]),
                (request, response + b"trailing"), (request, b"HTTP/1.1 404 Not Found\r\n"),
                (request, response.replace(b"404 Not Found", b"200 OK"))):
            with self.subTest(request=supplied_request[:30], response_bytes=len(supplied_response)), self.assertRaises(ValueError):
                decode_missing_keeper_response(supplied_request, supplied_response, identity)


    def test_missing_keeper_chunked_error_requires_each_delimiter_and_final_blank_line(self):
        identity = "a" * 64
        request = ("GET /v1.47/containers/" + identity + "/json HTTP/1.1\r\n"
            "Host: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode()
        body = raw_checks.encoded({"message": "No such container: " + identity})
        head = b"HTTP/1.1 404 Not Found\r\nTransfer-Encoding: chunked\r\n\r\n"
        chunk = format(len(body), "x").encode() + b"\r\n" + body
        complete = head + chunk + b"\r\n0\r\n\r\n"
        self.assertEqual(decode_missing_keeper_response(request, complete, identity), engine.strict_json_loads(body))
        for malformed in (head + chunk + b"XX0\r\n\r\n", head + chunk + b"\r\n0\r\n",
                complete + b"trailing", head + chunk[:-1] + b"\r\n0\r\n\r\n"):
            with self.subTest(response_bytes=len(malformed)), self.assertRaises(ValueError):
                decode_missing_keeper_response(request, malformed, identity)


    def test_c05_rejects_prior_continuity_error_even_when_cli_bytes_completed(self):
        attempted = {"kind": "cli", "step_index": 3, "authenticated": True, "cli_removed": True,
            "process": {"status": "completed", "completion": {"natural": True}, "capture_complete": True}}
        assert_c05_attempt_eligible(self, attempted)
        for value in ({**attempted, "authenticated": False},
                {**attempted, "observation_error": "Role lifecycle state differs"},
                {**attempted, "observation_error": "CLI state volume identity differs"},
                {**attempted, "observation_error": "Registered immutable Git source changed"},
                {**attempted, "observation_error": ""}):
            with self.subTest(row=value), self.assertRaises(AssertionError):
                assert_c05_attempt_eligible(self, value)

    def test_c05_rejects_unproved_retirement_or_incomplete_process(self):
        attempted = {"kind": "cli", "step_index": 3, "authenticated": True, "cli_removed": True,
            "process": {"status": "completed", "completion": {"natural": True}, "capture_complete": True}}
        for value in ({**attempted, "cli_removed": False}, {**attempted, "cleanup_error": "retirement unavailable"},
                {**attempted, "process": {**attempted["process"], "capture_complete": False}},
                {**attempted, "process": {**attempted["process"], "completion": {"natural": False}}}):
            with self.subTest(row=value), self.assertRaises(AssertionError):
                assert_c05_attempt_eligible(self, value)
