"""Inert HTTP lifecycle controls: real Git/journals, fake Engine/socket only."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import socket
import threading
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_http_execution_v1 as frozen_v1
from gossip_harness import candidate_http_execution_v2 as execution
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness.gitstore import GitStore

IMAGE = "sha256:" + "a" * 64
ENDPOINT = execution.engine.EngineEndpoint("/fixture/docker.sock", 1, 2)
RUNTIME = {"protocol": execution.engine.PROTOCOL, "endpoint": asdict(ENDPOINT), "api_version": "1.47",
    "os": "linux", "engine_version": "27.4.0", "engine_git_commit": "6bc6209b88a7a834c91f77d848e025c79e0227a1",
    "cgroup_version": "2", "cgroup_driver": "cgroupfs", "oom_kill_disable_supported": False,
    "daemon_id": "fixture", "architecture": "aarch64", "kernel_version": "fixture", "image_id": IMAGE,
    "image_inspect_sha256": "d" * 64}
# Derive exact public pinned compatibility gate instead of inventing another runtime.
RUNTIME.update({key: value for key, value in execution.engine.startup_policy()["runtime_gate"].items()
                if key in ("engine_version", "api_version", "os", "cgroup_version")})
REQUIREMENTS = next(iter(execution.finite.SUPPORTED_REQUIREMENTS_SHA256))


def request() -> dict:
    return {"method": "GET", "target": "/marker", "headers": [["Host", "127.0.0.1:8765"],
        ["Connection", "close"], ["Content-Length", "0"]], "body_b64": ""}


def recipe(*, restart=False) -> execution.HttpRecipe:
    steps = [execution.HttpStep("start-1", "start"), execution.HttpStep("request-1", "probe", execution.encoded(request())),
             execution.HttpStep("stop-1", "stop")]
    if restart:
        steps += [execution.HttpStep("start-2", "start"), execution.HttpStep("request-2", "probe", execution.encoded(request())),
                  execution.HttpStep("stop-2", "stop")]
    return execution.HttpRecipe("fixture-http", ("python", "/workspace/server.py", "--db", "/tmp/library.sqlite",
        "--root", "/inputs", "--port", "8765"), (("input.txt", b"fixture"),), ("empty",), tuple(steps))


def role(kind="server", *, name=None) -> execution.RoleSpec:
    return execution.RoleSpec(kind, name or "fixture-" + kind,
        execution.PROBE_ARGV if kind == "probe" else recipe().server_argv if kind == "server" else ("python", "-I", "-c", "pass"),
        tuple(sorted({"gossip.execution": "fixture", "gossip.role": kind, "gossip.source": "c" * 64,
            "gossip.fixture": "e" * 64, "gossip.helper": wire.helper_sha256(), "gossip.epoch": "1"}.items())),
        (("/probe", str(Path('/fixture/probe').resolve())),) if kind == "probe" else
        (("/workspace", str(Path('/fixture/source').resolve())), ("/inputs", str(Path('/fixture/inputs').resolve()))) if kind == "server" else (),
        "" if kind == "probe" else "fixture-volume", "b" * 64 if kind == "probe" else "")


def inspection(spec, container_id=None, state="created") -> dict:
    container_id = container_id or ("a" * 64 if spec.role == "probe" else "b" * 64)
    env = ["HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"]
    env += [key + "=" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
            "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")]
    result = {"Id": container_id, "Name": "/" + spec.name, "Created": "2026-10-03T00:00:00.001Z",
        "Image": IMAGE, "Path": spec.argv[0], "Args": list(spec.argv[1:]), "RestartCount": 0,
        "Config": {"Image": IMAGE, "User": "65534:65534", "WorkingDir": "/workspace" if spec.role == "server" else "/",
            "Tty": False, "OpenStdin": False, "StdinOnce": False, "AttachStdout": True, "AttachStderr": True,
            "Entrypoint": [spec.argv[0]], "Cmd": list(spec.argv[1:]), "Healthcheck": {"Test": ["NONE"]},
            "Labels": dict(spec.labels), "Env": env, "Hostname": container_id[:12], "Domainname": ""},
        "HostConfig": {"NetworkMode": "container:" + spec.server_id if spec.role == "probe" else "none",
            "ReadonlyRootfs": True, "Privileged": False, "OomKillDisable": False, "AutoRemove": False,
            "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024, "NanoCpus": 1000000000,
            "PidsLimit": 64, "CapDrop": ["ALL"], "Init": False, "SecurityOpt": ["no-new-privileges:true"],
            "LogConfig": {"Type": "none", "Config": {}}, "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
            "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}], "IpcMode": "private", "CgroupnsMode": "private",
            "Tmpfs": {"/tmp": execution.PROBE_TMPFS} if spec.role == "probe" else None},
        "Mounts": [{"Destination": target, "Type": "bind", "Source": source, "RW": False, "Propagation": "rprivate"}
                   for target, source in spec.binds],
        "State": {"Status": "created", "Running": False, "Paused": False, "Restarting": False, "OOMKilled": False,
            "Dead": False, "Pid": 0, "ExitCode": 0, "Error": "", "StartedAt": "0001-01-01T00:00:00Z",
            "FinishedAt": "0001-01-01T00:00:00Z"}}
    if spec.role != "probe":
        result["Mounts"].append({"Destination": "/tmp", "Type": "volume", "Name": spec.volume,
            "Driver": "local", "RW": spec.role == "server"})
    transition(result, state)
    return result


def transition(value, state):
    value["State"].update(Status=state, Running=state == "running", Pid=123 if state == "running" else 0)
    if state != "created":
        value["HostConfig"]["OomKillDisable"] = None
        value["State"]["StartedAt"] = "2026-10-03T00:00:01.002Z"
    if state == "exited":
        value["State"]["FinishedAt"] = "2026-10-03T00:00:02.003Z"


def transcript(limits) -> bytes:
    raw = wire.request_bytes(request(), 8765, limits)
    header = b"sl local_address st inode\n"
    snap = {"byteorder": "little", "tables": {name: {"status": "ok", "raw": wire._record(header), "errno": None}
                                                  for name in ("tcp", "tcp6")}}
    return wire.encoded({"protocol": wire.PROTOCOL, "input_sha256": execution.sha256(wire.build_probe_input(request(), 8765, limits)),
        "request_sha256": execution.sha256(raw), "sent": wire._record(raw),
        "received": wire._record(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"), "connected": True,
        "socket_eof": False, "termination": "message_complete", "errno": None, "elapsed_seconds": 0.01,
        "io_operations": 3, "connect_attempts": 1, "connect_refused_attempts": 0,
        "listeners_before_stage": "connected_pre_request", "listeners_before": snap, "listeners_after": snap})


def donor_fixture(*, current_change=None, baseline_change=None, epoch=1):
    binding = execution.binding_for({"server.py": b"inert"}, recipe(), execution.HttpPolicy(IMAGE), RUNTIME,
                                    requirements_sha256=REQUIREMENTS)
    binding = replace(binding, source_sha256="c" * 64, fixture_sha256="e" * 64)
    registration = execution.HttpRegistration(binding, "1" * 40, "2" * 40, "fixture-donor")
    spec = role("server")
    baseline = inspection(spec, state="running"); current = deepcopy(baseline)
    if baseline_change: baseline_change(baseline)
    if current_change: current_change(current)
    return execution.bind_probe_donor(baseline=baseline, current=current, server_spec=spec,
        registration=registration, runtime=RUNTIME, epoch=epoch)


class HttpRoleProfileV2Tests(unittest.TestCase):
    def test_closed_roles_accept_exact_profiles(self):
        for kind in ("server", "keeper", "probe"):
            spec = role(kind)
            execution.validate_role(inspection(spec), spec, IMAGE, RUNTIME)
            execution.validate_state(inspection(spec), "created")

    def test_probe_shares_only_exact_server_network(self):
        spec = role("probe")
        for key, value in (("NetworkMode", "none"), ("NetworkMode", "host"), ("NetworkMode", "container:" + "c" * 64),
                           ("PidMode", "container:" + "b" * 64), ("IpcMode", "host"), ("Privileged", True),
                           ("PortBindings", {"8765/tcp": [{"HostPort": "8765"}]})):
            row = inspection(spec)
            row["HostConfig"][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                execution.validate_role(row, spec, IMAGE, RUNTIME)

    def test_probe_rejects_candidate_database_and_mutable_helper_mounts(self):
        spec = role("probe")
        for change in ("extra", "writable", "source", "duplicate"):
            row = inspection(spec)
            if change == "extra": row["Mounts"].append(inspection(role())["Mounts"][-1])
            if change == "writable": row["Mounts"][0]["RW"] = True
            if change == "source": row["Mounts"][0]["Source"] = "/foreign"
            if change == "duplicate": row["Mounts"].append(deepcopy(row["Mounts"][0]))
            with self.subTest(change=change), self.assertRaises(ValueError):
                execution.validate_role(row, spec, IMAGE, RUNTIME)

    def test_keeper_is_source_free_and_readonly_state(self):
        spec = role("keeper")
        row = inspection(spec)
        row["Mounts"][0]["RW"] = True
        with self.assertRaises(ValueError): execution.validate_role(row, spec, IMAGE, RUNTIME)
        with self.assertRaises(ValueError): replace(spec, binds=role().binds)

    def test_server_network_none_and_frozen_finite_validator_not_relaxed(self):
        row = inspection(role())
        row["HostConfig"]["NetworkMode"] = "bridge"
        with self.assertRaises(ValueError): execution.validate_role(row, role(), IMAGE, RUNTIME)
        with self.assertRaises(ValueError):
            execution.engine.validate_sandbox(inspection(role("probe")), execution.engine.ProcessPolicy(IMAGE),
                                              expected_argv=list(execution.PROBE_ARGV), runtime=RUNTIME)

    def test_startup_oom_transform_is_typed_runtime_gated_and_phase_limited(self):
        spec = role()
        before, after = inspection(spec), inspection(spec, state="running")
        self.assertTrue(execution.role_identity_comparison(before, after, spec, RUNTIME, "created-to-running")["matches"])
        for value in (0, True, "false"):
            altered = deepcopy(after); altered["HostConfig"]["OomKillDisable"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                execution.role_identity_comparison(before, altered, spec, RUNTIME, "created-to-running")
        with self.assertRaises(ValueError): execution.role_identity_comparison(before, after, spec, RUNTIME, "created-to-prestart")
        with self.assertRaises(ValueError): execution.role_identity_comparison(before, after, spec, dict(RUNTIME, engine_version="other"), "created-to-running")

    def test_mount_order_only_permutation_preserves_every_complete_row(self):
        spec = role(); before = inspection(spec, state="running"); after = deepcopy(before)
        after["Mounts"].reverse()
        self.assertTrue(execution.role_identity_comparison(before, after, spec, RUNTIME, "running-to-running")["matches"])
        after["Mounts"][0]["foreign_field"] = "different"
        self.assertFalse(execution.role_identity_comparison(before, after, spec, RUNTIME, "running-to-running")["matches"])
        self.assertEqual(before["Mounts"][0]["Destination"], "/workspace")

    def test_same_server_epoch_requires_pid_started_at_and_zero_restarts(self):
        spec = role(); before = inspection(spec, state="running")
        for key, value in (("Pid", 124), ("StartedAt", "2026-10-03T00:00:01.003Z")):
            after = deepcopy(before); after["State"][key] = value
            self.assertFalse(execution.role_identity_comparison(before, after, spec, RUNTIME, "running-to-running")["matches"])
        after = deepcopy(before); after["RestartCount"] = 1
        with self.assertRaises(ValueError): execution.validate_state(after, "running")

    def test_controlled_stop_does_not_require_exit_zero(self):
        row = inspection(role(), state="exited"); row["State"]["ExitCode"] = 143
        execution.validate_state(row, "exited")
        execution.role_identity_comparison(inspection(role(), state="running"), row, role(), RUNTIME, "running-to-exited")

    def test_missing_cmd_is_distinct_from_explicit_singleton_null(self):
        spec = replace(role("keeper"), argv=("python",))
        row = inspection(spec); row["Config"]["Cmd"] = None
        execution.validate_role(row, spec, IMAGE, RUNTIME)
        del row["Config"]["Cmd"]
        with self.assertRaises(ValueError): execution.validate_role(row, spec, IMAGE, RUNTIME)

    def test_literal_create_configs_have_no_shell_or_extra_namespace_access(self):
        for kind in ("server", "keeper", "probe"):
            spec = role(kind); argv = execution.create_argv(spec, IMAGE)
            self.assertEqual(argv[-len(spec.argv):], [IMAGE, *spec.argv[1:]])
            self.assertNotIn("--init", argv); self.assertNotIn("--rm", argv)
            self.assertNotIn("--pid=host", argv); self.assertNotIn("--privileged", argv)
            if kind == "probe":
                self.assertIn("--network=container:" + "b" * 64, argv)
                self.assertFalse(any("/workspace" in x or "fixture-volume" in x for x in argv))


class HttpControllerJournalV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("http-controller-v1-inert", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git", {"server.py": "raise RuntimeError('must never execute')\n"})
        cls.commit = cls.store.head(); cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.root = self.artifacts.root.resolve() / self.id().split(".")[-1]
        self.policy = execution.HttpPolicy(IMAGE); self.recipe = recipe()
        self.binding = execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"}, requirements_sha256=REQUIREMENTS)
        self.registration = execution.HttpRegistration(self.binding, self.commit, self.tree, "offline-1")
        self.controllers = []
        self.addCleanup(lambda: [x.close() for x in self.controllers])

    def controller(self, **kwargs):
        item = execution.CandidateHttpExecution(self.root, self.store, self.registration, self.recipe, self.policy, mode="fixture", **kwargs)
        self.controllers.append(item); return item

    def test_real_git_snapshot_binds_complete_source_without_candidate_import(self):
        with patch.object(execution.engine, "runtime_identity", side_effect=AssertionError("Engine forbidden")):
            controller = self.controller()
        self.assertEqual(controller.files, self.files)
        self.assertEqual(controller.config["source_manifest"], execution.source_manifest(self.files))
        self.assertEqual(controller.config["helper_sha256"], wire.helper_sha256())

    def test_wrong_commit_tree_binding_rejected(self):
        self.registration = replace(self.registration, tree_oid="f" * 40)
        with self.assertRaises(execution.ExecutionError): self.controller()

    def test_qualification_purpose_and_milestone_are_closed(self):
        for change in ({"purpose": "independent_acceptance"}, {"purpose": "repeatability"}, {"milestone": "M4"}, {"requirements_sha256": "b" * 64}):
            with self.subTest(change=change), self.assertRaises(execution.ExecutionError): replace(self.binding, **change)

    def test_ordered_recipe_rejects_restarts_before_stop_or_probe_without_server(self):
        for steps in (tuple(execution.HttpStep("p-" + str(i), "probe", execution.encoded(request())) for i in range(3)),
                      (execution.HttpStep("a", "start"), execution.HttpStep("b", "start"), execution.HttpStep("c", "stop")),
                      self.recipe.steps[:-1] + (execution.HttpStep("p", "probe", execution.encoded(request())),)):
            with self.subTest(steps=steps), self.assertRaises(ValueError): replace(self.recipe, steps=steps)
        self.assertEqual(len(recipe(restart=True).steps), 6)

    def test_fixture_and_argv_collisions_and_path_escapes_rejected(self):
        for changes in ({"fixtures": (("../escape", b"x"),)}, {"fixtures": (("a", b"x"), ("a/b", b"y"))},
                        {"fixtures": (("a", b"x"),), "directories": ("a",)}, {"database_path": "/elsewhere/db"},
                        {"server_argv": ("python", "server.py")}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): replace(self.recipe, **changes)

    def test_helper_envelope_is_bound_before_dispatch(self):
        self.assertGreater(wire.max_probe_output_bytes(self.policy.wire_limits), 4 * 1024 * 1024)
        with self.assertRaises(ValueError): replace(self.policy, stream_limit_bytes=4 * 1024 * 1024)
        with self.assertRaises(ValueError): replace(self.policy, probe_timeout_seconds=10)

    def test_existing_intent_is_never_redispatched(self):
        controller = self.controller()
        with patch.object(controller, "_dispatch", side_effect=AssertionError("dispatch forbidden")):
            with self.assertRaisesRegex(execution.ExecutionError, "Fixture journal"): controller.execute_once()
            with self.assertRaises(execution.ExecutionUnknown): controller.execute_once()
        self.assertTrue((self.root / "intent.json").is_file())

    def test_rollback_and_foreign_suffix_rejected_by_external_checkpoint(self):
        controller = self.controller(); old = controller.checkpoint()
        controller._retain("new.json", execution.encoded({"x": 1})); checkpoint = controller.checkpoint(); controller.close()
        with self.assertRaises(execution.ExecutionError): self.controller(expected_checkpoint=old)
        (self.root / "new.json").unlink()
        with self.assertRaises(execution.ExecutionError): self.controller(expected_checkpoint=checkpoint)

    def test_current_checkpoint_resumes_but_requires_external_authority(self):
        controller = self.controller(); checkpoint = controller.checkpoint(); controller.close()
        with self.assertRaises(execution.ExecutionError): self.controller()
        resumed = self.controller(expected_checkpoint=checkpoint)
        self.assertEqual(resumed.checkpoint(), checkpoint)

    def test_journal_tamper_and_duplicate_output_rejected(self):
        controller = self.controller(); controller._retain("one.json", b"{}")
        with self.assertRaises(FileExistsError): controller._retain("one.json", b"{}")
        (self.root / "one.json").write_bytes(b'{"x":1}')
        with self.assertRaises(execution.ExecutionError): controller.checkpoint()

    def test_loaded_source_change_and_foreign_thread_rejected(self):
        controller = self.controller()
        with patch.object(execution, "evaluator_sources", return_value={}):
            with self.assertRaises(execution.ExecutionError): controller._unchanged()
        errors = []
        def foreign():
            try: controller.checkpoint()
            except execution.ExecutionError as error: errors.append(str(error))
        thread = threading.Thread(target=foreign); thread.start(); thread.join()
        self.assertEqual(len(errors), 1)

    def test_mismatched_recipe_source_and_helper_pins_rejected(self):
        for key in ("source_sha256", "recipe_sha256", "helper_sha256", "fixture_sha256", "limits_sha256",
                    "runtime_sha256", "environment_sha256", "evaluator_sha256", "seed_sha256"):
            with self.subTest(key=key):
                binding = replace(self.binding, **{key: "f" * 64})
                registration = replace(self.registration, binding=binding)
                with self.assertRaises(execution.ExecutionError):
                    execution.CandidateHttpExecution(self.root / key, self.store, registration, self.recipe, self.policy, mode="fixture")


class FakeAttach:
    def __init__(self, payload, gate, *, early_eof=False):
        self.buffer = bytearray(); self.raw = bytearray(b"inert-attach"); self.eof = False
        self.socket = self; self.payload = payload; self.gate = gate; self.sent_payload = False
        self.early_eof = early_eof
    def send(self, value): pass
    def headers(self):
        return 101, {"connection": "Upgrade", "upgrade": "tcp", "content-type": "application/vnd.docker.raw-stream"}
    def settimeout(self, value): pass
    def recv(self, size, flags=0):
        if self.early_eof: return b""
        raise BlockingIOError()
    def _timeout(self): pass
    def receive(self):
        self.gate.wait(timeout=2)
        if self.sent_payload: self.eof = True
        else:
            self.buffer.extend(self.payload); self.raw.extend(self.payload); self.sent_payload = True
    def shutdown(self, how): self.gate.set()
    def close(self): pass


class HttpProbeLifecycleV2Tests(unittest.TestCase):
    def run_fixture(self, *, early_eof=False, final_change=None, start_status=204, payload=b"{}"): 
        spec = role("probe"); before = inspection(spec); after = inspection(spec, state="exited")
        donor = donor_fixture(); after["Config"]["Hostname"] = donor.hostname
        if final_change: final_change(after)
        gate = threading.Event(); retained = {}; starts = []
        frame = b"\x01\0\0\0" + len(payload).to_bytes(4, "big") + payload
        attached = FakeAttach(frame, gate, early_eof=early_eof)
        def save(name, raw):
            self.assertNotIn(name, retained); retained[name] = raw
        def control(endpoint, method, path, *, deadline, retain, label, on_response=None):
            starts.append(path); retain(label + "-request.bin", b"inert start")
            retain(label + "-response.bin", b"inert complete response")
            if on_response: on_response(start_status)
            gate.set(); return start_status, b""
        def json_control(endpoint, path, *, deadline, retain, label, method="GET"):
            result = before if label == "prestart" else {"StatusCode": 0} if label == "wait" else after
            retain(label + "-response.bin", execution.encoded(result)); return deepcopy(result)
        with patch.object(execution.engine, "_Wire", return_value=attached), \
                patch.object(execution.engine, "_control", side_effect=control), \
                patch.object(execution.engine, "_json_control", side_effect=json_control):
            result = execution.run_probe(ENDPOINT, expected=before, spec=spec, policy=execution.HttpPolicy(IMAGE),
                                         runtime=RUNTIME, retain=save, label="probe", donor=donor)
        return result, retained, starts

    def test_attached_fixed_helper_has_independent_natural_completion(self):
        result, retained, starts = self.run_fixture()
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["completion"]["natural"])
        self.assertTrue(result["attach_eof_after_start_confirmation"])
        self.assertEqual(retained["probe-stdout.bin"], b"{}")
        self.assertEqual(len(starts), 1)
        self.assertIn("probe-attach-response.bin", retained)

    def test_eof_before_start_prevents_dispatch(self):
        result, retained, starts = self.run_fixture(early_eof=True)
        self.assertEqual(starts, [])
        self.assertEqual(result["status"], "completion_unproven")
        self.assertFalse(result["completion"]["natural"])

    def test_bad_start_acknowledgement_never_becomes_completion(self):
        result, retained, starts = self.run_fixture(start_status=500)
        self.assertEqual(len(starts), 1)
        self.assertEqual(result["start_response"]["status"], 500)
        self.assertEqual(result["status"], "completion_unproven")
        self.assertFalse(result["completion"]["natural"])

    def test_helper_network_drift_and_nonzero_exit_are_unavailable(self):
        changes = [lambda row: row["HostConfig"].update(NetworkMode="none"),
                   lambda row: row["State"].update(ExitCode=2), lambda row: row.update(RestartCount=1)]
        for change in changes:
            with self.subTest(change=change):
                result, _, _ = self.run_fixture(final_change=change)
                self.assertEqual(result["status"], "completion_unproven")

    def test_server_role_cannot_enter_finite_probe_runner(self):
        with patch.object(execution.engine, "_Wire", side_effect=AssertionError("no IO")):
            with self.assertRaises(ValueError):
                execution.run_probe(ENDPOINT, expected=inspection(role()), spec=role(), policy=execution.HttpPolicy(IMAGE),
                                    runtime=RUNTIME, retain=lambda *_: None, label="probe", donor=donor_fixture())


class FakeEngineHistory:
    """Inert control seam. Never touches Docker, sockets, provider APIs or keys."""
    def __init__(self, controller, *, defect=None):
        self.controller = controller; self.defect = defect; self.specs = {}; self.values = {}; self.volumes = {}
        self.events = []; self.probe_calls = 0; self.counter = 0; self.donor_before_create = []
    def argv(self, spec, image):
        self.specs[spec.name] = spec
        return self.original_argv(spec, image)
    def command(self, label, argv, timeout=None):
        self.events.append(tuple(argv)); data = b""; code = 0
        if argv[1:3] == ["container", "ls"]:
            name = argv[-1][len("name=^/"):-1]; data = self.values.get(name, {}).get("Id", "").encode()
        elif argv[1:3] == ["volume", "ls"]:
            name = argv[-1][len("name=^"):-1]; data = name.encode() if name in self.volumes else b""
        elif argv[1:3] == ["volume", "create"]:
            name = argv[-1]; intent = execution._json(self.controller.root / "intent.json")
            self.volumes[name] = {"Name": name, "Driver": "local", "Options": execution.VOLUME_OPTIONS,
                "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}}
            data = name.encode()
        elif argv[1:3] == ["volume", "inspect"]:
            data = execution.encoded(self.volumes[argv[-1]])
        elif argv[1:3] == ["volume", "rm"]:
            del self.volumes[argv[-1]]; data = argv[-1].encode()
        elif argv[1] == "create":
            name = argv[argv.index("--name") + 1]; spec = self.specs[name]
            if spec.role == "probe":
                step = label.removesuffix("-probe-create")
                donor_path = self.controller.root / (step + "-donor.json")
                donor = execution._json(donor_path)
                assert donor["server_id"] == spec.server_id
                assert donor["inspection"] == next(v for v in self.values.values() if v["Id"] == spec.server_id)
                assert self.controller.checkpoint().files
                self.donor_before_create.append(donor)
            self.counter += 1; cid = format(self.counter, "012x") + format(self.counter, "052x")
            self.values[name] = inspection(spec, cid); data = cid.encode()
        elif argv[1] == "inspect":
            if argv[-1] not in self.values: code = 1
            else: data = execution.encoded(self.values[argv[-1]])
        elif argv[1] == "rm":
            name = next(name for name, value in self.values.items() if value["Id"] == argv[-1])
            if self.defect == "cleanup" and self.specs[name].role == "probe":
                raise execution.ExecutionError("inert cleanup ownership mismatch")
            del self.values[name]; data = argv[-1].encode()
        else: raise AssertionError(argv)
        record = {"argv": argv, "exit_code": code, "timed_out": False, "capture_complete": True}
        for kind, raw in (("stdout", data), ("stderr", b"")):
            path = label + "-" + kind + ".bin"; self.controller._retain(path, raw)
            record[kind] = {"path": path, "sha256": execution.sha256(raw), "bytes": len(raw), "observed_bytes": len(raw), "truncated": False}
        self.controller._retain(label + ".json", execution.encoded(record)); return record
    def inspect(self, label, cid):
        result = deepcopy(next(value for value in self.values.values() if value["Id"] == cid))
        self.controller._retain(label + "-inert-inspection.json", execution.encoded(result)); return result
    def control(self, label, cid, operation):
        value = next(value for value in self.values.values() if value["Id"] == cid)
        transition(value, "running" if operation == "start" else "exited")
        self.events.append((operation, cid)); self.controller._retain(label + "-inert-response.bin", b"204")
        return 204, b""
    def _after_probe(self, cid):
        self.probe_calls += 1
        if self.defect in ("source-change", "fixture-change") and self.probe_calls == 2:
            spec = next(s for s in self.specs.values() if s.role == "probe" and self.values.get(s.name, {}).get("Id") == cid)
            server_spec = next(s for s in self.specs.values() if s.role == "server" and self.values.get(s.name, {}).get("Id") == spec.server_id)
            mount = "/workspace" if self.defect == "source-change" else "/inputs"
            path = Path(dict(server_spec.binds)[mount]) / ("server.py" if mount == "/workspace" else "input.txt")
            path.chmod(0o644); path.write_bytes(b"changed\n")
        if self.defect == "server-death":
            spec = next(s for s in self.specs.values() if s.role == "probe" and self.values.get(s.name, {}).get("Id") == cid)
            server = next(v for v in self.values.values() if v["Id"] == spec.server_id); transition(server, "exited")
        if self.defect == "runtime-change" and self.probe_calls == 2:
            execution.engine.runtime_identity.return_value = dict(RUNTIME, daemon_id="changed")
    def wire_factory(self, endpoint, deadline, limit):
        payload = transcript(self.controller.policy.wire_limits)
        frame = b"\x01\0\0\0" + len(payload).to_bytes(4, "big") + payload
        self.gate = threading.Event()
        return FakeAttach(frame, self.gate)
    def engine_control(self, endpoint, method, path, *, deadline, retain, label, on_response=None):
        cid = path.split("/")[2]
        value = next(v for v in self.values.values() if v["Id"] == cid)
        transition(value, "running")
        spec = next(s for s in self.specs.values() if s.name == value["Name"][1:])
        donor = next(v for v in self.values.values() if v["Id"] == spec.server_id)
        value["Config"]["Hostname"] = donor["Config"]["Hostname"]
        value["Config"]["Domainname"] = donor["Config"]["Domainname"]
        if self.defect == "spoof-hostname": value["Config"]["Hostname"] = "spoofed"
        if self.defect == "domain-change": value["Config"]["Domainname"] = "foreign"
        retain(label + "-request.bin", b"inert engine POST /start")
        retain(label + "-response.bin", b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
        if on_response: on_response(204)
        self.gate.set(); return 204, b""
    def engine_json(self, endpoint, path, *, deadline, retain, label, method="GET"):
        cid = path.split("/")[2]
        value = next(v for v in self.values.values() if v["Id"] == cid)
        if label == "wait":
            transition(value, "exited")
            if self.defect == "helper-nonzero": value["State"]["ExitCode"] = 2
            result = {"StatusCode": value["State"]["ExitCode"]}
        else:
            result = deepcopy(value)
            if label == "final": self._after_probe(cid)
        retain(label + "-response.bin", execution.encoded(result)); return result
    def enter(self):
        self.original_argv = execution.create_argv
        self.patches = [patch.object(execution, "create_argv", side_effect=self.argv),
                        patch.object(self.controller, "_command", side_effect=self.command),
                        patch.object(self.controller, "_inspect", side_effect=self.inspect),
                        patch.object(self.controller, "_control", side_effect=self.control),
                        patch.object(execution.engine, "_Wire", side_effect=self.wire_factory),
                        patch.object(execution.engine, "_control", side_effect=self.engine_control),
                        patch.object(execution.engine, "_json_control", side_effect=self.engine_json)]
        for item in self.patches: item.start()
    def close(self):
        for item in reversed(self.patches): item.stop()


class HttpHistoryOrchestrationV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("http-history-v2-inert", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git", {"server.py": "raise RuntimeError('no candidate on host')\n"})
        cls.commit = cls.store.head(); cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)
    def history(self, *, restart=False, defect=None):
        root = self.artifacts.root.resolve() / self.id().split(".")[-1]
        policy = execution.HttpPolicy(IMAGE); rec = recipe(restart=restart)
        runtime_patch = patch.object(execution.engine, "runtime_identity", return_value=RUNTIME); runtime_patch.start()
        self.addCleanup(runtime_patch.stop)
        binding = execution.binding_for(self.files, rec, policy, RUNTIME, requirements_sha256=REQUIREMENTS)
        registration = execution.HttpRegistration(binding, self.commit, self.tree, "inert-physical-path")
        controller = execution.CandidateHttpExecution(root, self.store, registration, rec, policy, endpoint=ENDPOINT)
        self.addCleanup(controller.close)
        backend = FakeEngineHistory(controller, defect=defect); backend.enter(); self.addCleanup(backend.close)
        return controller, backend

    def test_complete_start_probe_stop_cleanup_path_is_journaled_once(self):
        controller, backend = self.history(); result = controller.execute_once()
        self.assertEqual(result.status, "completed"); self.assertTrue(result.cleanup_verified)
        self.assertEqual(result.observations[0]["wire"].response.body, b"{}")
        event_count = len(backend.events)
        reused = controller.execute_once()
        self.assertEqual(reused.terminal_sha256, result.terminal_sha256)
        self.assertEqual(len(backend.events), event_count)
        self.assertEqual(backend.probe_calls, 1)
        row = result.observations[0]
        change = row["process"]["identity_comparison"]["changed_fields"][0]
        self.assertEqual(change["path"], "Config.Hostname")
        self.assertNotEqual(change["before"], change["after"])
        self.assertEqual(change["before"], row["probe_id"][:12])
        self.assertEqual(change["after"], row["donor"]["hostname"])
        self.assertEqual(backend.donor_before_create, [row["donor"]])
        self.assertEqual(row["donor"]["epoch"], row["epoch"])
        self.assertEqual(row["donor"]["binding"], asdict(controller.binding))
        self.assertEqual(row["process"]["identity_comparison"]["donor_sha256"], execution.digest(row["donor"]))

    def test_explicit_restart_has_new_server_same_keeper_volume_and_no_overlap(self):
        controller, backend = self.history(restart=True); result = controller.execute_once()
        self.assertEqual(result.status, "completed")
        terminal = execution._json(controller.root / "terminal.json"); first, second = terminal["epochs"]
        self.assertNotEqual(first["server_id"], second["server_id"])
        for key in ("database_volume", "source_sha256", "fixture_sha256", "keeper_id", "keeper_started_at"):
            self.assertEqual(first[key], second[key])
        removal = next(i for i, event in enumerate(backend.events) if event[:2] == ("docker", "rm") and event[-1] == first["server_id"])
        start = backend.events.index(("start", second["server_id"]))
        self.assertLess(removal, start); self.assertEqual(terminal["created_servers"], 2)
        self.assertEqual(terminal["observed_requests"], 2)
        for observation in result.observations:
            self.assertEqual(observation["donor"]["server_id"], observation["server_id"])
            self.assertEqual(observation["donor"]["epoch"], observation["epoch"])
            change = observation["process"]["identity_comparison"]["changed_fields"][0]
            self.assertNotEqual(change["before"], change["after"])

    def test_source_change_withholds_current_row_but_preserves_prior_observation(self):
        controller, backend = self.history(restart=True, defect="source-change"); result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(result.observations[0]["wire"].response.body, b"{}")
        self.assertFalse(result.observations[1]["authenticated"])
        self.assertIsNone(result.observations[1]["wire"])
        self.assertIn("Staged files", result.observations[1]["observation_error"])

    def test_fixture_change_withholds_only_current_row_authentication(self):
        controller, backend = self.history(restart=True, defect="fixture-change"); result = controller.execute_once()
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertFalse(result.observations[1]["authenticated"])
        self.assertIn("Staged files", result.observations[1]["observation_error"])

    def test_changed_runtime_after_probe_withholds_row_and_final_reauthentication(self):
        controller, backend = self.history(restart=True, defect="runtime-change")
        with self.assertRaisesRegex(ValueError, "Runtime/environment/recipe"): controller.execute_once()
        terminal = execution._json(controller.root / "terminal.json")
        self.assertTrue(terminal["observations"][0]["authenticated"])
        self.assertFalse(terminal["observations"][1]["authenticated"])
        self.assertIn("Runtime/environment/recipe", terminal["observations"][1]["observation_error"])

    def test_failed_helper_completion_is_not_a_sent_request_census(self):
        controller, backend = self.history(defect="helper-nonzero"); result = controller.execute_once()
        self.assertFalse(result.observations[0]["authenticated"])
        terminal = execution._json(controller.root / "terminal.json")
        self.assertEqual(terminal["observed_probe_records"], 1)
        self.assertEqual(terminal["observed_requests"], 0)
        self.assertEqual(terminal["unknown_request_outcomes"], 1)
        self.assertTrue(result.cleanup_verified)

    def test_server_death_keeps_raw_probe_and_owned_cleanup_without_provenance(self):
        controller, backend = self.history(defect="server-death"); result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable"); self.assertTrue(result.cleanup_verified)
        self.assertFalse(result.observations[0]["authenticated"])
        self.assertTrue((controller.root / result.observations[0]["process"]["stdout"]["path"]).is_file())

    def test_cleanup_exception_keeps_previously_authenticated_response_facts(self):
        controller, backend = self.history(defect="cleanup"); result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable"); self.assertFalse(result.cleanup_verified)
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(result.observations[0]["wire"].response.body, b"{}")
        self.assertIn("cleanup_error", result.observations[0])

    def test_created_count_does_not_claim_failed_or_unknown_creation(self):
        controller, backend = self.history()
        original = backend.command
        def fail_create(label, argv, timeout=None):
            if argv[1] == "create":
                name = argv[argv.index("--name") + 1]
                if backend.specs[name].role == "probe":
                    raise execution.ExecutionError("inert lost probe creation")
            return original(label, argv, timeout)
        with patch.object(controller, "_command", side_effect=fail_create): result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        terminal = execution._json(controller.root / "terminal.json")
        self.assertEqual(terminal["create_intents"], 3)
        self.assertEqual(terminal["created_keepers"], 1)
        self.assertEqual(terminal["created_servers"], 1)
        self.assertEqual(terminal["created_helpers"], 0)
        self.assertEqual(terminal["unconfirmed_creates"], 1)
        self.assertEqual(terminal["observed_requests"], 0)


    def test_spoofed_helper_hostname_withholds_composed_wire_authentication(self):
        controller, backend = self.history(defect="spoof-hostname"); result = controller.execute_once()
        self.assertFalse(result.observations[0]["authenticated"])
        self.assertIsNone(result.observations[0]["wire"])
        self.assertIn("hostname", result.observations[0]["process"]["error"])
        self.assertEqual(backend.probe_calls, 1)
        self.assertTrue(result.cleanup_verified)

    def test_changed_helper_domain_withholds_composed_wire_authentication(self):
        controller, backend = self.history(defect="domain-change"); result = controller.execute_once()
        self.assertFalse(result.observations[0]["authenticated"])
        self.assertIn("empty domain", result.observations[0]["process"]["error"])
        self.assertEqual(backend.probe_calls, 1)
        self.assertTrue(result.cleanup_verified)


class HttpProbeDonorV2Tests(unittest.TestCase):
    def pair(self):
        spec = role("probe"); before = inspection(spec); after = inspection(spec, state="exited")
        donor = donor_fixture(); after["Config"]["Hostname"] = donor.hostname
        return spec, before, after, donor

    def compare(self, before, after, spec, donor, *, runtime=None, phase="created-to-exited"):
        return execution.role_identity_comparison(before, after, spec, runtime or RUNTIME, phase, donor=donor)

    def test_exact_retained_failure_pair_is_future_rule_only_and_v1_still_rejects(self):
        path = Path(__file__).resolve().parents[1] / "fixtures/candidate-http-probe-hostname-v1/inspection-records.json"
        fixture = json.loads(path.read_bytes()); values = {key: row["value"] for key, row in fixture["records"].items()}
        def spec_from(module, value):
            return module.RoleSpec(**{**value, "argv": tuple(value["argv"]),
                "binds": tuple(tuple(x) for x in value["binds"]), "labels": tuple(tuple(x) for x in value["labels"])})
        original = values["config"]["registration"]
        binding = execution.HttpBinding(**{**original["binding"], "protocol": execution.PROTOCOL,
            "evaluator_sha256": execution.digest(execution.evaluator_sources()),
            "role_policy_sha256": execution.digest(execution.role_policy_definition()),
            "role_policy_source_sha256": execution.role_policy_identity()["source_sha256"]})
        registration = execution.HttpRegistration(binding, original["commit_oid"], original["tree_oid"], "retained-data-offline")
        runtime = values["config"]["runtime"]
        donor = execution.bind_probe_donor(baseline=values["server_baseline"], current=values["server_current"],
            server_spec=spec_from(execution, values["server_intent"]["spec"]), registration=registration, runtime=runtime, epoch=1)
        before, after = values["probe_created"], values["probe_exited"]
        self.assertNotEqual(before["Config"]["Hostname"], after["Config"]["Hostname"])
        raw_snapshot = execution.encoded([before, after, values["server_current"]])
        result = self.compare(before, after, spec_from(execution, values["probe_intent"]["spec"]), donor, runtime=runtime)
        self.assertTrue(result["matches"])
        self.assertEqual(result["changed_fields"][0]["after"], donor.hostname)
        old = frozen_v1.role_identity_comparison(before, after,
            spec_from(frozen_v1, values["probe_intent"]["spec"]), runtime, "created-to-exited")
        self.assertFalse(old["matches"])
        self.assertEqual(execution.encoded([before, after, values["server_current"]]), raw_snapshot)

    def test_evidence_is_complete_immutable_and_keeps_raw_comparison_values(self):
        spec, before, after, donor = self.pair()
        result = self.compare(before, after, spec, donor)
        self.assertTrue(result["matches"])
        self.assertIn("pinned-runtime-probe-donor-hostname", result["transformations"])
        self.assertEqual(result["before_sha256"], execution.digest(before))
        self.assertEqual(result["after_sha256"], execution.digest(after))
        self.assertEqual(result["donor_sha256"], execution.digest(donor.record()))
        inspection_copy = donor.inspection; inspection_copy["Config"]["Hostname"] = "spoofed"
        self.assertNotEqual(donor.hostname, "spoofed")

    def test_no_donor_or_hash_only_dictionary_cannot_authorize_hostname_change(self):
        for donor_value in (None, {"inspection_sha256": "a" * 64}):
            spec, before, after, _ = self.pair()
            with self.subTest(donor=donor_value), self.assertRaises(ValueError):
                self.compare(before, after, spec, donor_value)

    def test_spoofed_and_unchanged_own_hostname_are_rejected(self):
        for hostname in ("spoofed", "a" * 12):
            spec, before, after, donor = self.pair(); after["Config"]["Hostname"] = hostname
            with self.subTest(hostname=hostname), self.assertRaises(ValueError): self.compare(before, after, spec, donor)
        spec, before, after, donor = self.pair(); before["Config"]["Hostname"] = donor.hostname
        with self.assertRaises(ValueError): self.compare(before, after, spec, donor)

    def test_missing_null_and_wrong_type_fields_do_not_normalize(self):
        for side in ("before", "after"):
            for key in ("Hostname", "Domainname"):
                for marker in ("missing", None, 0, False, [], {}):
                    spec, before, after, donor = self.pair(); row = before if side == "before" else after
                    if marker == "missing": del row["Config"][key]
                    else: row["Config"][key] = marker
                    with self.subTest(side=side, key=key, marker=marker), self.assertRaises(ValueError):
                        self.compare(before, after, spec, donor)

    def test_changed_domain_is_rejected_in_donor_created_and_exited(self):
        with self.assertRaises(ValueError):
            donor_fixture(current_change=lambda row: row["Config"].update(Domainname="foreign"))
        for side in ("before", "after"):
            spec, before, after, donor = self.pair(); row = before if side == "before" else after
            row["Config"]["Domainname"] = "foreign"
            with self.subTest(side=side), self.assertRaises(ValueError): self.compare(before, after, spec, donor)

    def test_wrong_donor_id_epoch_network_target_or_source_labels_fail(self):
        for change in ("id", "epoch", "network", "source", "fixture", "execution", "helper"):
            spec, before, after, donor = self.pair()
            if change == "id":
                spec = replace(spec, server_id="c" * 64)
                before["HostConfig"]["NetworkMode"] = after["HostConfig"]["NetworkMode"] = "container:" + spec.server_id
            elif change == "network": after["HostConfig"]["NetworkMode"] = "container:" + "c" * 64
            else:
                labels = dict(spec.labels); labels["gossip." + change] = "wrong"
                spec = replace(spec, labels=tuple(sorted(labels.items())))
                before["Config"]["Labels"] = after["Config"]["Labels"] = labels
            with self.subTest(change=change), self.assertRaises(ValueError): self.compare(before, after, spec, donor)
        with self.assertRaises(ValueError): donor_fixture(epoch=2)

    def test_missing_changed_or_dead_current_server_is_not_a_donor(self):
        changes = [lambda row: row.pop("Config"), lambda row: row.update(Id="c" * 64),
            lambda row: row["Config"].update(Hostname="different"),
            lambda row: row["State"].update(Pid=999), lambda row: transition(row, "exited")]
        for change in changes:
            with self.subTest(change=change), self.assertRaises((ValueError, KeyError)):
                donor_fixture(current_change=change)
        donor = donor_fixture()
        with self.assertRaises(ValueError): replace(donor, inspection_json=b"{}")
        with self.assertRaises(ValueError): replace(donor, inspection_json=None)

    def test_unsupported_runtime_and_wrong_role_or_phase_have_no_donor_authority(self):
        spec, before, after, donor = self.pair()
        with self.assertRaises(ValueError): self.compare(before, after, spec, donor, runtime=dict(RUNTIME, engine_version="other"))
        for phase in ("created-to-prestart", "created-to-running", "running-to-running", "running-to-exited"):
            spec, before, after, donor = self.pair()
            with self.subTest(phase=phase), self.assertRaises(ValueError): self.compare(before, after, spec, donor, phase=phase)
        for kind in ("server", "keeper"):
            candidate_spec = role(kind)
            with self.subTest(role=kind), self.assertRaises(ValueError):
                self.compare(inspection(candidate_spec), inspection(candidate_spec, state="exited"), candidate_spec, donor)
            with self.assertRaises(ValueError): replace(donor, server_spec=candidate_spec if kind == "keeper" else role("probe"))

    def test_prestart_and_running_hostname_drift_remains_exact_without_donor(self):
        for kind, phase, state in (("probe", "created-to-prestart", "created"),
                                   ("server", "running-to-running", "running"),
                                   ("keeper", "running-to-running", "running")):
            spec = role(kind); before = inspection(spec, state=state); after = deepcopy(before)
            after["Config"]["Hostname"] = "different"
            self.assertFalse(execution.role_identity_comparison(before, after, spec, RUNTIME, phase)["matches"])

    def test_unrelated_config_host_and_complete_mount_fields_remain_exact(self):
        for change in (lambda row: row["Config"].update(ExtraField="different"),
                       lambda row: row["HostConfig"].update(ExtraField="different"),
                       lambda row: row["Mounts"][0].update(ExtraField="different")):
            spec, before, after, donor = self.pair(); change(after)
            with self.subTest(change=change): self.assertFalse(self.compare(before, after, spec, donor)["matches"])

    def test_policy_definition_and_source_are_bound_in_registration_and_donor(self):
        donor = donor_fixture(); identity = execution.role_policy_identity()
        self.assertEqual(donor.registration.binding.role_policy_sha256, identity["definition_sha256"])
        self.assertEqual(donor.registration.binding.role_policy_source_sha256, identity["source_sha256"])
        for key in ("role_policy_sha256", "role_policy_source_sha256", "runtime_sha256"):
            registration = replace(donor.registration, binding=replace(donor.registration.binding, **{key: "f" * 64}))
            with self.subTest(key=key), self.assertRaises(ValueError): replace(donor, registration=registration)
