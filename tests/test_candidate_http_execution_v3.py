"""Versioned HTTP v3 controls: real Git/journals, inert Engine/socket only."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import socket
import threading
import time
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_http_execution_v1 as frozen_v1
from gossip_harness import candidate_http_execution_v3 as execution
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_cases_core_v1 as core
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
        execution.PROBE_ARGV if kind == "probe" else recipe().server_argv if kind in ("server", "cli") else ("python", "-I", "-c", "pass"),
        tuple(sorted({"gossip.execution": "fixture", "gossip.role": kind, "gossip.source": "c" * 64,
            "gossip.fixture": "e" * 64, "gossip.helper": wire.helper_sha256(), "gossip.epoch": "1"}.items())),
        (("/probe", str(Path('/fixture/probe').resolve())),) if kind == "probe" else
        (("/workspace", str(Path('/fixture/source').resolve())), ("/inputs", str(Path('/fixture/inputs').resolve()))) if kind in ("server", "cli") else (),
        "" if kind == "probe" else "fixture-volume", "b" * 64 if kind == "probe" else "")


def inspection(spec, container_id=None, state="created") -> dict:
    container_id = container_id or ("a" * 64 if spec.role == "probe" else "b" * 64)
    env = ["HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"]
    env += [key + "=" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
            "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")]
    result = {"Id": container_id, "Name": "/" + spec.name, "Created": "2026-10-03T00:00:00.001Z",
        "Image": IMAGE, "Path": spec.argv[0], "Args": list(spec.argv[1:]), "RestartCount": 0,
        "Config": {"Image": IMAGE, "User": "65534:65534", "WorkingDir": "/workspace" if spec.role in ("server", "cli") else "/",
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
            "Driver": "local", "RW": spec.role in ("server", "cli")})
    transition(result, state)
    return result


def transition(value, state):
    value["State"].update(Status=state, Running=state == "running", Pid=123 if state == "running" else 0)
    if state != "created":
        value["HostConfig"]["OomKillDisable"] = None
        value["State"]["StartedAt"] = "2026-10-03T00:00:01.002Z"
    if state == "exited":
        value["State"]["FinishedAt"] = "2026-10-03T00:00:02.003Z"


def transcript(limits, *, req=None, port=8765) -> bytes:
    req = request() if req is None else req
    raw = wire.request_bytes(req, port, limits)
    header = b"sl local_address st inode\n"
    snap = {"byteorder": "little", "tables": {name: {"status": "ok", "raw": wire._record(header), "errno": None}
                                                  for name in ("tcp", "tcp6")}}
    return wire.encoded({"protocol": wire.PROTOCOL, "input_sha256": execution.sha256(wire.build_probe_input(req, port, limits)),
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


def mixed_recipe(*, changed_root=False) -> execution.HttpRecipe:
    original = recipe()
    cli_argv = ("python", "/workspace/server.py", "--db", original.database_path,
                "--root", "/inputs", "status")
    second_root = "/inputs/other" if changed_root else "/inputs"
    second_argv = tuple(second_root if value == "/inputs" else value for value in original.server_argv)
    steps = original.steps + (execution.HttpStep("cli-1", "cli", epoch=1, root_path="/inputs", argv=cli_argv),
        execution.HttpStep("start-2", "start", epoch=2, root_path=second_root, argv=second_argv),
        execution.HttpStep("request-2", "probe", execution.encoded(request()), epoch=2, root_path=second_root),
        execution.HttpStep("stop-2", "stop", epoch=2, root_path=second_root))
    return replace(original, steps=steps, directories=("empty", "other"), input_entries=None)


class HttpRoleProfileV3Tests(unittest.TestCase):
    def test_closed_roles_accept_exact_profiles(self):
        for kind in ("server", "keeper", "probe", "cli"):
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
        for kind in ("server", "keeper", "probe", "cli"):
            spec = role(kind); argv = execution.create_argv(spec, IMAGE)
            self.assertEqual(argv[-len(spec.argv):], [IMAGE, *spec.argv[1:]])
            self.assertNotIn("--init", argv); self.assertNotIn("--rm", argv)
            self.assertNotIn("--pid=host", argv); self.assertNotIn("--privileged", argv)
            if kind == "probe":
                self.assertIn("--network=container:" + "b" * 64, argv)
                self.assertFalse(any("/workspace" in x or "fixture-volume" in x for x in argv))


class HttpLiteralMechanicsV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = catalog.definitions()

    def test_all_literal_cases_map_without_shortening_or_reordering(self):
        recipes = tuple(execution.recipe_from_case(case) for case in self.cases)
        self.assertEqual(len(recipes), 276)
        self.assertEqual(len({item.recipe_id for item in recipes}), 276)
        self.assertEqual([item.row_id for item in recipes], list(catalog.ROW_IDS))
        total_steps = total_cli = 0
        for case, converted in zip(self.cases, recipes):
            with self.subTest(row=case.row_id):
                self.assertNotIn("/", converted.recipe_id)
                self.assertEqual(converted.definition_json, core.encode(case.record()))
                self.assertEqual(execution.sha256(converted.definition_json), case.definition_sha256)
                self.assertEqual(converted.input_entries, case.fixtures)
                self.assertEqual([step.step_id for step in converted.steps], [step.step_id for step in case.steps])
                for original, adapted in zip(case.steps, converted.steps):
                    self.assertEqual(adapted.kind, "probe" if original.kind == "request" else original.kind)
                    self.assertEqual(adapted.epoch, original.epoch)
                    self.assertEqual(adapted.root_path, original.root)
                    self.assertEqual(adapted.argv, original.argv)
                    self.assertEqual(adapted.declaration_json, core.encode(original.record()))
                    if original.request:
                        self.assertEqual(adapted.request_json, execution.encoded(original.request.recipe()))
                        self.assertEqual(wire.request_bytes(json.loads(adapted.request_json), converted.port),
                                         original.request.wire_bytes())
                total_steps += len(converted.steps)
                total_cli += sum(step.kind == "cli" for step in converted.steps)
        self.assertEqual(total_steps, 8203)
        self.assertEqual(total_cli, 29)
        self.assertEqual(max(map(lambda item: len(item.steps), recipes)), 87)
        self.assertEqual(sum(len(item.steps) > 64 for item in recipes), 4)

    def test_runtime_step_ceiling_is_128_and_preserves_longest_literal_history(self):
        longest = max(self.cases, key=lambda item: len(item.steps))
        adapted = execution.recipe_from_case(longest)
        self.assertEqual(len(adapted.steps), 87)
        self.assertEqual(sum(step.kind == "cli" for step in adapted.steps), 29)
        self.assertEqual(execution.MAX_STEPS, 128)
        base = recipe()
        probes = tuple(execution.HttpStep("request-" + str(index), "probe", execution.encoded(request()))
                       for index in range(126))
        accepted = replace(base, steps=(base.steps[0], *probes, base.steps[-1]))
        self.assertEqual(len(accepted.steps), 128)
        with self.assertRaises(ValueError):
            replace(base, steps=(base.steps[0], *probes,
                execution.HttpStep("overflow", "probe", execution.encoded(request())), base.steps[-1]))

    def test_legacy_steps_resolve_to_explicit_epoch_root_and_server_argv(self):
        adapted = recipe(restart=True)
        self.assertEqual([step.epoch for step in adapted.steps], [1, 1, 1, 2, 2, 2])
        self.assertTrue(all(step.root_path == "/inputs" for step in adapted.steps))
        self.assertEqual(adapted.steps[0].argv, adapted.server_argv)
        self.assertEqual(adapted.steps[3].argv, adapted.server_argv)

    def test_literal_cli_requires_removed_server_and_exact_epoch_root(self):
        base = mixed_recipe()
        cli = base.steps[3]
        invalid = (
            (cli, *base.steps),
            (base.steps[0], cli, *base.steps[1:]),
            (*base.steps[:3], replace(cli, epoch=2), *base.steps[4:]),
            (*base.steps[:3], replace(cli, root_path="/inputs/empty"), *base.steps[4:]),
        )
        for steps in invalid:
            with self.subTest(steps=steps), self.assertRaises(ValueError):
                replace(base, steps=steps)

    def test_per_epoch_root_argv_are_literal_and_database_stays_the_same(self):
        adapted = mixed_recipe(changed_root=True)
        starts = [step for step in adapted.steps if step.kind == "start"]
        self.assertEqual([step.root_path for step in starts], ["/inputs", "/inputs/other"])
        self.assertEqual([step.argv[step.argv.index("--root") + 1] for step in starts],
                         ["/inputs", "/inputs/other"])
        for field, value in (("root_path", "/inputs/missing"),
                             ("argv", tuple("/tmp/other.sqlite" if item == adapted.database_path else item
                                            for item in starts[1].argv))):
            changed = list(adapted.steps)
            changed[4] = replace(starts[1], **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                replace(adapted, steps=tuple(changed))

    def test_oversized_product_body_remains_within_unchanged_evaluator_wire_bound(self):
        case = next(case for case in self.cases if any(step.request and len(step.request.body) == 65537
                                                      for step in case.steps))
        adapted = execution.recipe_from_case(case)
        requests = [json.loads(step.request_json) for step in adapted.steps if step.kind == "probe"]
        self.assertEqual(max(len(wire.request_bytes(item, adapted.port)) for item in requests), 65661)
        self.assertEqual(execution.wire.WireLimits().request_limit_bytes, 131072)


    def test_selected_wire_policy_rejects_request_before_registration(self):
        case = next(case for case in self.cases if any(step.request and len(step.request.body) == 65537
                                                      for step in case.steps))
        adapted = execution.recipe_from_case(case)
        policy = replace(execution.HttpPolicy(IMAGE), wire_limits=replace(wire.WireLimits(), request_limit_bytes=65536))
        with patch.object(execution.engine, "runtime_identity", side_effect=AssertionError("Engine forbidden")):
            with self.assertRaises(ValueError):
                execution.binding_for({"server.py": b"inert"}, adapted, policy, RUNTIME,
                                      requirements_sha256=REQUIREMENTS)

    def test_literal_definition_and_executable_request_cannot_diverge(self):
        adapted = execution.recipe_from_case(self.cases[0])
        index = next(index for index, step in enumerate(adapted.steps) if step.kind == "probe")
        altered = json.loads(adapted.steps[index].request_json)
        altered["target"] = "/different"
        steps = list(adapted.steps)
        steps[index] = replace(steps[index], request_json=execution.encoded(altered))
        with self.assertRaises(ValueError):
            replace(adapted, steps=tuple(steps))
        with self.assertRaises(ValueError):
            replace(adapted, row_id="HTTP-OTHER/different")

    def test_cli_role_rejects_foreign_volume_mount_network_and_argv(self):
        spec = role("cli")
        changes = (
            lambda row: row["Mounts"][-1].update(Name="foreign-volume"),
            lambda row: row["Mounts"][0].update(Source="/foreign-source"),
            lambda row: row["Mounts"][1].update(RW=True),
            lambda row: row["HostConfig"].update(NetworkMode="container:" + "b" * 64),
            lambda row: row["Config"].update(WorkingDir="/"),
            lambda row: row["Config"].update(Cmd=["different"]),
            lambda row: row["Config"]["Labels"].update({"gossip.epoch": "2"}),
        )
        for change in changes:
            value = inspection(spec)
            change(value)
            with self.subTest(change=change), self.assertRaises(ValueError):
                execution.validate_role(value, spec, IMAGE, RUNTIME)

    def test_cli_never_borrows_probe_donor_hostname_normalization(self):
        spec = role("cli")
        before = inspection(spec)
        after = inspection(spec, state="exited")
        after["Config"]["Hostname"] = "foreign"
        result = execution.role_identity_comparison(before, after, spec, RUNTIME, "created-to-exited")
        self.assertFalse(result["matches"])
        with self.assertRaises(ValueError):
            execution.role_identity_comparison(before, after, spec, RUNTIME, "created-to-exited", donor=donor_fixture())


class HttpControllerJournalV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("http-controller-v3-inert", retain_success=True)
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
            if "server_argv" in changes:
                changes["steps"] = tuple(replace(step, argv=()) if step.kind == "start" else step
                                         for step in self.recipe.steps)
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


    def test_controller_journal_reads_over_engine_limit_without_relaxing_engine(self):
        controller = self.controller()
        payload = {"text": "x" * (1024 * 1024 + 1)}
        raw = execution.encoded(payload)
        controller._retain("large-row.json", raw)
        self.assertEqual(execution._json(controller.root / "large-row.json"), payload)
        self.assertEqual(controller._row(controller._descriptor("large-row.json")), payload)
        with self.assertRaises(execution.engine.ProcessError):
            execution.engine.strict_json_loads(raw)

    def test_row_descriptors_require_exact_bytes_hash_and_closed_fields(self):
        controller = self.controller()
        controller._retain("row.json", execution.encoded({"step_id": "one"}))
        descriptor = controller._descriptor("row.json")
        for changed in (dict(descriptor, bytes=descriptor["bytes"] + 1),
                        dict(descriptor, sha256="f" * 64), dict(descriptor, extra=True),
                        dict(descriptor, path="../row.json")):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                controller._row(changed)
        self.assertEqual(controller._row(descriptor), {"step_id": "one"})

    def test_operation_admission_preserves_declared_byte_and_file_cleanup_capacity(self):
        controller = self.controller()
        original_bytes = controller._retained_bytes
        controller._retained_bytes = (execution.MAX_JOURNAL_BYTES - execution.CLEANUP_RESERVE_BYTES
                                      - execution.OPERATION_LIMIT_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "protected journal capacity"):
            controller._begin_operation()
        controller._retained_bytes = original_bytes
        with patch.object(execution, "MAX_JOURNAL_FILES",
                          len(controller._authenticated) + execution.CLEANUP_RESERVE_FILES + execution.OPERATION_LIMIT_FILES - 1):
            with self.assertRaisesRegex(ValueError, "protected journal capacity"):
                controller._begin_operation()
        controller._begin_operation()
        self.assertEqual(controller._operation_bytes, original_bytes)
        self.assertEqual(controller._operation_files, len(controller._authenticated))

    def test_byte_quota_rejection_leaves_reserved_cleanup_and_terminal_writable(self):
        controller = self.controller()
        limit = controller._retained_bytes + execution.CLEANUP_RESERVE_BYTES + 1
        with patch.object(execution, "MAX_JOURNAL_BYTES", limit):
            with self.assertRaisesRegex(ValueError, "protected resource bound"):
                controller._retain("blocked.json", b"{}")
            self.assertFalse((controller.root / "blocked.json").exists())
            controller._cleanup_mode = True
            controller._retain("owned-cleanup.json", b"{}")
            controller._retain("terminal.json", b"{}")
            self.assertTrue((controller.root / "terminal.json").is_file())
            with self.assertRaises(ValueError):
                controller._begin_operation()
        self.assertEqual(dict(controller.checkpoint().files), controller._authenticated)

    def test_file_quota_rejection_leaves_reserved_cleanup_slots_writable(self):
        controller = self.controller()
        limit = len(controller._authenticated) + execution.CLEANUP_RESERVE_FILES
        with patch.object(execution, "MAX_JOURNAL_FILES", limit):
            with self.assertRaisesRegex(ValueError, "protected resource bound"):
                controller._retain("blocked.json", b"{}")
            controller._cleanup_mode = True
            controller._retain("owned-cleanup.json", b"{}")
            controller._retain("terminal.json", b"{}")
        self.assertEqual(dict(controller.checkpoint().files), controller._authenticated)

    def test_operation_byte_and_file_allocations_reject_before_creating_excess_file(self):
        controller = self.controller()
        with patch.object(execution, "OPERATION_LIMIT_BYTES", 3), patch.object(execution, "OPERATION_LIMIT_FILES", 2):
            controller._begin_operation()
            controller._retain("first.bin", b"123")
            with self.assertRaisesRegex(ValueError, "Operation observation allocation"):
                controller._retain("overflow.bin", b"4")
            self.assertFalse((controller.root / "overflow.bin").exists())
            controller._retain("second.bin", b"")
            with self.assertRaisesRegex(ValueError, "Operation observation allocation"):
                controller._retain("third.bin", b"")
            self.assertFalse((controller.root / "third.bin").exists())

    def test_finite_window_and_operation_deadline_keep_cleanup_time_separate(self):
        controller = self.controller()
        controller._work_deadline = 100.0
        controller._history_deadline = 100.0 + execution.CLEANUP_SECONDS
        with patch.object(execution.time, "monotonic", return_value=99.0):
            self.assertEqual(controller._remaining(10), 1)
            self.assertEqual(controller._deadline_after(10), 100)
            with self.assertRaisesRegex(ValueError, "finite CLI"):
                controller._finite_window()
            controller._cleanup_mode = True
            self.assertEqual(controller._remaining(10), 10)
        controller._cleanup_mode = False
        with patch.object(execution.time, "monotonic", return_value=100.0):
            with self.assertRaisesRegex(ValueError, "deadline exhausted"):
                controller._begin_operation()
        with self.assertRaises(ValueError):
            replace(self.policy, lifetime_seconds=execution.CLEANUP_SECONDS)

    def test_normal_retention_cannot_spend_compact_terminal_reserve(self):
        controller = self.controller()
        with self.assertRaisesRegex(ValueError, "cleanup terminal"):
            controller._retain("terminal.json", b"{}")
        controller._cleanup_mode = True
        with patch.object(execution, "TERMINAL_LIMIT_BYTES", 1):
            with self.assertRaisesRegex(ValueError, "cleanup terminal"):
                controller._retain("terminal.json", b"{}")
        self.assertFalse((controller.root / "terminal.json").exists())


    def test_command_wait_reap_and_reader_joins_share_one_absolute_deadline(self):
        controller = self.controller()
        controller.mode, controller.endpoint = "physical", ENDPOINT
        waits = []
        joins = []
        class Child:
            stdout = object()
            stderr = object()
            returncode = -9
            def wait(self, *, timeout):
                waits.append(timeout)
                if len(waits) == 1:
                    raise execution.subprocess.TimeoutExpired("inert-docker-control", timeout)
                return self.returncode
            def kill(self):
                pass
        class Worker:
            def __init__(self, **_kwargs):
                pass
            def start(self):
                pass
            def join(self, *, timeout):
                joins.append(timeout)
            def is_alive(self):
                return False
        with patch.object(execution.engine.EngineEndpoint, "validate"), \
             patch.object(execution.subprocess, "Popen", return_value=Child()), \
             patch.object(execution.threading, "Thread", side_effect=Worker), \
             patch.object(execution.time, "monotonic", side_effect=(100.0, 101.0, 102.0, 103.0, 104.0)):
            result = controller._command("deadline-control", ["docker", "version"], timeout=10)
        self.assertEqual(result["observation_deadline_monotonic"], 110)
        self.assertEqual(waits, [9, 8])
        self.assertEqual(joins, [7, 6])
        self.assertTrue(result["timed_out"])


    def test_repeated_maximum_argv_epochs_keep_terminal_within_reserved_capacity(self):
        base = recipe()
        prefix = (*base.server_argv, "--inert-padding")
        argv = (*prefix, "x" * (execution.finite.MAX_ARGV_BYTES - sum(len(item.encode()) for item in prefix)))
        self.assertEqual(sum(len(item.encode()) for item in argv), 65536)
        steps = []
        for epoch in range(1, 64):
            steps.append(execution.HttpStep("start-" + str(epoch), "start", epoch=epoch,
                                            root_path="/inputs", argv=argv))
            if epoch in (1, 63):
                steps.append(execution.HttpStep("request-" + str(epoch), "probe", execution.encoded(request()),
                                                epoch=epoch, root_path="/inputs"))
            steps.append(execution.HttpStep("stop-" + str(epoch), "stop", epoch=epoch, root_path="/inputs"))
        self.recipe = replace(base, server_argv=argv, steps=tuple(steps))
        self.assertEqual(len(self.recipe.steps), execution.MAX_STEPS)
        self.binding = execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"},
                                            requirements_sha256=REQUIREMENTS)
        self.registration = execution.HttpRegistration(self.binding, self.commit, self.tree, "epoch-capacity")
        with patch.object(execution.engine, "runtime_identity", side_effect=AssertionError("Engine forbidden")):
            controller = self.controller()
        records, descriptors = [], []
        for index, step in enumerate(self.recipe.steps):
            if step.kind != "start":
                continue
            label = "step-" + str(index).zfill(3)
            record = {"epoch": step.epoch, "step_label": label, "argv": list(step.argv),
                      "root_path": step.root_path, "server_id": format(step.epoch, "064x")}
            name = label + "-epoch.json"
            controller._retain(name, execution.encoded(record))
            records.append(record)
            descriptors.append(controller._descriptor(name))
        self.assertEqual(len(descriptors), 63)
        self.assertGreater(len(execution.encoded({"epochs": records})), execution.TERMINAL_LIMIT_BYTES)
        compact = execution.encoded({"protocol": execution.PROTOCOL, "epochs": descriptors})
        self.assertLess(len(compact), execution.TERMINAL_LIMIT_BYTES // 8)
        controller._cleanup_mode = True
        controller._retain("terminal.json", compact)
        retained = execution._json(controller.root / "terminal.json")
        self.assertEqual([controller._row(item) for item in retained["epochs"]], records)
        self.assertEqual(dict(controller.checkpoint().files), controller._authenticated)


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


class HttpProbeLifecycleV3Tests(unittest.TestCase):
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
        self.events = []; self.probe_calls = 0; self.cli_calls = 0; self.counter = 0; self.donor_before_create = []
    def argv(self, spec, image):
        self.specs[spec.name] = spec
        return self.original_argv(spec, image)
    def command(self, label, argv, timeout=None):
        self.events.append(tuple(argv)); data = b""; code = 0
        if argv[1:3] == ["container", "ls"]:
            name = argv[-1][len("name=^/"):-1]; data = self.values.get(name, {}).get("Id", "").encode()
            if self.defect == "server-absence" and "-retire-absence" in label and self.specs[name].role == "server":
                data = b"unknown-retained-container"
        elif argv[1:3] == ["volume", "ls"]:
            name = argv[-1][len("name=^"):-1]; data = name.encode() if name in self.volumes else b""
        elif argv[1:3] == ["volume", "create"]:
            name = argv[-1]; intent = execution._json(self.controller.root / "intent.json")
            self.volumes[name] = {"Name": name, "Driver": "local", "Options": execution.VOLUME_OPTIONS,
                "CreatedAt": "2026-10-03T00:00:00.000Z", "Scope": "local",
                "Mountpoint": "/var/lib/docker/volumes/" + name + "/_data", "FixtureExtra": {"identity": "same"},
                "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}}
            data = name.encode()
        elif argv[1:3] == ["volume", "inspect"]:
            volume = deepcopy(self.volumes[argv[-1]])
            if self.defect == "cli-volume-change" and "cli-volume-after" in label:
                volume["CreatedAt"] = "2026-10-03T00:00:00.001Z"
            data = execution.encoded(volume)
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
            self.values[name] = inspection(spec, cid); self.last_created = cid; data = cid.encode()
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
        value = next(value for value in self.values.values() if value["Id"] == self.last_created)
        spec = self.specs[value["Name"][1:]]
        if spec.role == "cli":
            payload = b'{"fixture":"finite-outcome"}\n'
        else:
            probe_input = json.loads((Path(dict(spec.binds)["/probe"]) / "request.json").read_bytes())
            payload = transcript(self.controller.policy.wire_limits,
                                 req=probe_input["recipe"], port=probe_input["port"])
        frame = b"\x01\0\0\0" + len(payload).to_bytes(4, "big") + payload
        self.gate = threading.Event()
        return FakeAttach(frame, self.gate)
    def engine_control(self, endpoint, method, path, *, deadline, retain, label, on_response=None):
        cid = path.split("/")[2]
        value = next(v for v in self.values.values() if v["Id"] == cid)
        transition(value, "running")
        spec = next(s for s in self.specs.values() if s.name == value["Name"][1:])
        if spec.role == "probe":
            donor = next(v for v in self.values.values() if v["Id"] == spec.server_id)
            value["Config"]["Hostname"] = donor["Config"]["Hostname"]
            value["Config"]["Domainname"] = donor["Config"]["Domainname"]
            if self.defect == "spoof-hostname": value["Config"]["Hostname"] = "spoofed"
            if self.defect == "domain-change": value["Config"]["Domainname"] = "foreign"
        else:
            self.events.append(("finite-start" if path.endswith("/start") else "finite-kill", cid))
            if not path.endswith("/start"):
                transition(value, "exited")
        retain(label + "-request.bin", b"inert engine POST /start")
        retain(label + "-response.bin", b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
        if on_response: on_response(204)
        self.gate.set(); return 204, b""
    def engine_json(self, endpoint, path, *, deadline, retain, label, method="GET"):
        cid = path.split("/")[2]
        value = next(v for v in self.values.values() if v["Id"] == cid)
        spec = self.specs[value["Name"][1:]]
        if label == "wait":
            if self.defect == "cli-timeout" and spec.role == "cli":
                raise TimeoutError("inert finite command deadline")
            transition(value, "exited")
            if self.defect == "helper-nonzero" and spec.role == "probe": value["State"]["ExitCode"] = 2
            if self.defect == "cli-nonzero" and spec.role == "cli": value["State"]["ExitCode"] = 2
            result = {"StatusCode": value["State"]["ExitCode"]}
        else:
            result = deepcopy(value)
            if label == "final": self._after_probe(cid)
            if label == "inspect-final" and spec.role == "cli":
                self.cli_calls += 1
                if self.defect == "cli-keeper-change":
                    keeper = next(item for name, item in self.values.items() if self.specs[name].role == "keeper")
                    keeper["State"]["StartedAt"] = "2026-10-03T00:00:01.004Z"
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


class HttpHistoryOrchestrationV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("http-history-v3-inert", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git", {"server.py": "raise RuntimeError('no candidate on host')\n"})
        cls.commit = cls.store.head(); cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)
    def history(self, *, restart=False, defect=None, supplied_recipe=None, supplied_policy=None):
        root = self.artifacts.root.resolve() / self.id().split(".")[-1]
        policy = supplied_policy or execution.HttpPolicy(IMAGE); rec = supplied_recipe or recipe(restart=restart)
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
        terminal = execution._json(controller.root / "terminal.json"); first, second = [controller._row(item) for item in terminal["epochs"]]
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
        self.assertIn("Staged", result.observations[1]["observation_error"])

    def test_fixture_change_withholds_only_current_row_authentication(self):
        controller, backend = self.history(restart=True, defect="fixture-change"); result = controller.execute_once()
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertFalse(result.observations[1]["authenticated"])
        self.assertIn("Regular file metadata differs", result.observations[1]["observation_error"])

    def test_changed_runtime_after_probe_withholds_row_and_final_reauthentication(self):
        controller, backend = self.history(restart=True, defect="runtime-change")
        with self.assertRaisesRegex(ValueError, "Runtime/environment/recipe"): controller.execute_once()
        terminal = execution._json(controller.root / "terminal.json")
        rows = [execution._json(controller.root / item["path"]) for item in terminal["observations"]]
        self.assertTrue(rows[0]["authenticated"])
        self.assertFalse(rows[1]["authenticated"])
        self.assertIn("Runtime/environment/recipe", rows[1]["observation_error"])

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


    def test_http_cli_http_keeps_one_volume_keeper_and_exact_finite_argv(self):
        declared = mixed_recipe(changed_root=True)
        controller, backend = self.history(supplied_recipe=declared)
        result = controller.execute_once()
        self.assertEqual(result.status, "completed")
        self.assertTrue(result.cleanup_verified)
        self.assertEqual(len(result.observations), 2)
        self.assertEqual(len(result.cli_observations), 1)
        cli = result.cli_observations[0]
        self.assertTrue(cli["authenticated"])
        self.assertTrue(cli["process"]["completion"]["natural"])
        self.assertEqual(cli["process"]["exit_code"], 0)
        terminal = execution._json(controller.root / "terminal.json")
        first, second = [controller._row(item) for item in terminal["epochs"]]
        for key in ("database_volume", "database_path", "source_sha256", "fixture_sha256", "keeper_id"):
            self.assertEqual(cli[key], first[key])
            self.assertEqual(first[key], second[key])
        cli_spec = next(item for item in backend.specs.values() if item.role == "cli")
        self.assertEqual(cli_spec.argv, declared.steps[3].argv)
        self.assertEqual(cli_spec.volume, first["database_volume"])
        self.assertEqual(sum(event[:3] == ("docker", "volume", "create") for event in backend.events), 1)
        self.assertEqual(backend.cli_calls, 1)
        self.assertEqual(backend.probe_calls, 2)
        process_intent = next(item for item in cli["process"]["evidence"] if item.endswith("-intent.json"))
        retained = execution._json(controller.root / process_intent)
        self.assertEqual(retained["expected_argv"], list(declared.steps[3].argv))
        self.assertEqual(terminal["created_cli"], 1)
        self.assertEqual(terminal["observed_requests"], 2)
        self.assertEqual(execution._json(controller.root / "intent.json")["planned_steps"], 7)
        second_spec = next(item for item in backend.specs.values()
                           if item.role == "server" and dict(item.labels)["gossip.epoch"] == "2")
        self.assertIn("/inputs/other", second_spec.argv)

    def test_natural_cli_exit_two_is_retained_outcome_without_product_verdict(self):
        controller, backend = self.history(supplied_recipe=mixed_recipe(), defect="cli-nonzero")
        result = controller.execute_once()
        self.assertEqual(result.status, "completed")
        self.assertTrue(result.cli_observations[0]["authenticated"])
        self.assertEqual(result.cli_observations[0]["process"]["exit_code"], 2)
        self.assertEqual(len(result.observations), 2)
        self.assertEqual(result.cli_observations[0]["product_verdict"], "not_evaluated")

    def test_unproved_server_removal_blocks_cli_and_successor_epoch(self):
        controller, backend = self.history(supplied_recipe=mixed_recipe(), defect="server-absence")
        result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(result.cli_observations, ())
        self.assertEqual(backend.cli_calls, 0)
        self.assertEqual(sum(item.role == "server" for item in backend.specs.values()), 1)
        self.assertEqual(sum(item.role == "cli" for item in backend.specs.values()), 0)

    def test_cli_timeout_preserves_first_http_and_blocks_successor_without_retry(self):
        controller, backend = self.history(supplied_recipe=mixed_recipe(), defect="cli-timeout")
        result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertEqual(len(result.observations), 1)
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(len(result.cli_observations), 1)
        self.assertFalse(result.cli_observations[0]["authenticated"])
        self.assertEqual(result.cli_observations[0]["process"]["status"], "timeout")
        self.assertEqual(sum(event[0] == "finite-start" for event in backend.events), 1)
        self.assertEqual(sum(item.role == "server" for item in backend.specs.values()), 1)
        self.assertTrue(result.cleanup_verified)

    def test_keeper_discontinuity_after_cli_retains_bytes_and_blocks_next_server(self):
        controller, backend = self.history(supplied_recipe=mixed_recipe(), defect="cli-keeper-change")
        result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(len(result.cli_observations), 1)
        cli = result.cli_observations[0]
        self.assertFalse(cli["authenticated"])
        self.assertTrue(cli["process"]["completion"]["natural"])
        self.assertTrue((controller.root / cli["process"]["stdout"]["path"]).is_file())
        self.assertEqual(sum(item.role == "server" for item in backend.specs.values()), 1)
        self.assertEqual(sum(event[:3] == ("docker", "volume", "create") for event in backend.events), 1)


    def test_operation_admission_failure_retains_prior_facts_and_owned_cleanup(self):
        controller, backend = self.history(restart=True)
        original = controller._begin_operation
        calls = 0
        def exhausted_after_first_probe():
            nonlocal calls
            calls += 1
            if calls == 4:
                raise execution.ExecutionError("inert protected journal capacity exhausted")
            return original()
        with patch.object(controller, "_begin_operation", side_effect=exhausted_after_first_probe):
            result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.cleanup_verified)
        self.assertEqual(len(result.observations), 1)
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(backend.probe_calls, 1)
        self.assertEqual(sum(item.role == "server" for item in backend.specs.values()), 1)
        self.assertTrue((controller.root / "terminal.json").is_file())
        self.assertTrue(any("protected journal capacity" in error for error in result.infrastructure))

    def test_terminal_observation_references_must_be_unique_and_chronological(self):
        controller, backend = self.history(restart=True)
        controller.execute_once()
        terminal = execution._json(controller.root / "terminal.json")
        original_json = execution._json
        for observations in (terminal["observations"] * 2, list(reversed(terminal["observations"]))):
            altered = dict(terminal, observations=observations)
            def read(path):
                return altered if path == controller.root / "terminal.json" else original_json(path)
            with self.subTest(observations=observations), patch.object(execution, "_json", side_effect=read):
                with self.assertRaises(ValueError):
                    controller.verified_execution()


    def observation_retention_failure(self, *, cli):
        declared = mixed_recipe() if cli else recipe(restart=True)
        controller, backend = self.history(supplied_recipe=declared)
        target = "step-003-cli-observation.json" if cli else "step-001-observation.json"
        original = controller._retain
        failures = 0
        def fail_selected_retention(name, raw):
            nonlocal failures
            if name == target:
                failures += 1
                raise execution.ExecutionError("inert observation allocation exhausted")
            return original(name, raw)
        with patch.object(controller, "_retain", side_effect=fail_selected_retention):
            result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.cleanup_verified)
        self.assertEqual(failures, 1)
        self.assertFalse((controller.root / target).exists())
        terminal = execution._json(controller.root / "terminal.json")
        self.assertFalse(any(item["path"] == target for key in ("steps", "observations", "cli_observations")
                             for item in terminal[key]))
        self.assertEqual(terminal["entered_step_indices"], list(range(4 if cli else 2)))
        self.assertEqual(terminal["unentered_step_ids"], [step.step_id for step in declared.steps[4 if cli else 2:]])
        self.assertEqual(terminal["attempted_probes"], 1)
        self.assertEqual(terminal["attempted_cli"], 1 if cli else 0)
        self.assertEqual(terminal["unknown_request_outcomes"], 0 if cli else 1)
        self.assertEqual(terminal["unavailable_cli_attempts"], 1 if cli else 0)
        attempts = terminal["cli_transport_attempts"] if cli else terminal["probe_transport_attempts"]
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["step_index"], 3 if cli else 1)
        self.assertTrue(any(name.startswith(attempts[0]["artifact_prefix"]) for name, _ in result.checkpoint.files))
        self.assertEqual(sum(item.role == "server" for item in backend.specs.values()), 1)
        self.assertTrue(any("observation allocation exhausted" in error for error in result.infrastructure))
        before = tuple(backend.events)
        repeated = controller.execute_once()
        self.assertEqual(repeated.terminal_sha256, result.terminal_sha256)
        self.assertEqual(tuple(backend.events), before)
        return result, controller

    def test_probe_observation_retention_failure_keeps_cleanup_terminal_without_missing_reference(self):
        result, controller = self.observation_retention_failure(cli=False)
        self.assertEqual(result.observations, ())
        self.assertTrue((controller.root / "step-001-probe-stdout.bin").is_file())

    def test_cli_observation_retention_failure_preserves_http_and_raw_cli_without_missing_reference(self):
        result, controller = self.observation_retention_failure(cli=True)
        self.assertEqual(len(result.observations), 1)
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(result.cli_observations, ())
        self.assertTrue((controller.root / "step-003-cli-process-stdout.bin").is_file())


    def test_cli_volume_metadata_change_withholds_cli_and_successor_server(self):
        controller, backend = self.history(supplied_recipe=mixed_recipe(), defect="cli-volume-change")
        result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.observations[0]["authenticated"])
        self.assertEqual(len(result.cli_observations), 1)
        self.assertFalse(result.cli_observations[0]["authenticated"])
        self.assertTrue(result.cli_observations[0]["process"]["completion"]["natural"])
        self.assertEqual(sum(item.role == "server" for item in backend.specs.values()), 1)
        self.assertTrue(result.cleanup_verified)

    def assert_full_history_capacity(self, case):
        declared = execution.recipe_from_case(case)
        controller, backend = self.history(supplied_recipe=declared)
        started = time.monotonic()
        result = controller.execute_once()
        elapsed = time.monotonic() - started
        terminal = execution._json(controller.root / "terminal.json")
        rows = [controller._row(item) for item in terminal["steps"]]
        self.assertEqual(result.status, "completed")
        self.assertTrue(result.cleanup_verified)
        self.assertEqual([row["step_id"] for row in rows], [step.step_id for step in case.steps])
        self.assertEqual(len(result.observations), case.counts["http_requests"])
        self.assertEqual(len(result.cli_observations), case.counts["finite_cli_processes"])
        self.assertEqual(backend.probe_calls, case.counts["http_requests"])
        self.assertEqual(backend.cli_calls, case.counts["finite_cli_processes"])
        for key, expected in (("created_helpers", case.counts["planned_probe_processes"]),
                              ("created_cli", case.counts["finite_cli_processes"]),
                              ("created_servers", case.counts["server_epochs"]),
                              ("created_keepers", 1), ("observed_requests", case.counts["http_requests"])):
            self.assertEqual(terminal[key], expected)
        files = tuple(controller.checkpoint().files)
        summary = {"schema": "candidate-http-v3-inert-capacity-v1", "mode": "inert-Engine-and-socket-seams",
            "row_id": case.row_id, "definition_sha256": case.definition_sha256,
            "elapsed_seconds": elapsed, "journal_bytes": sum((controller.root / name).stat().st_size for name, _ in files),
            "journal_files": len(files), "terminal_bytes": (controller.root / "terminal.json").stat().st_size,
            "counts": case.counts, "controller_status": result.status,
            "cleanup_verified": result.cleanup_verified, "terminal_sha256": result.terminal_sha256,
            "execution_binding": asdict(controller.binding), "evaluator_sources": controller.sources,
            "physical_engine_calls": 0, "candidate_executions": 0, "provider_calls": 0,
            "product_verdict": "not_evaluated"}
        retained = controller.root.parent / (controller.root.name + "-capacity.json")
        with retained.open("x") as output:
            json.dump(summary, output, indent=2, sort_keys=True)
            output.write("\n")
        self.assertLessEqual(summary["journal_files"], execution.MAX_JOURNAL_FILES)
        self.assertLessEqual(summary["journal_bytes"], execution.MAX_JOURNAL_BYTES)
        self.assertLessEqual(summary["terminal_bytes"], execution.TERMINAL_LIMIT_BYTES)

    def test_full_87_step_29_cli_literal_history_retains_one_complete_journal(self):
        case = max(catalog.definitions(), key=lambda item: len(item.steps))
        self.assertEqual(len(case.steps), 87)
        self.assertEqual(case.counts["finite_cli_processes"], 29)
        self.assert_full_history_capacity(case)

    def test_full_70_request_literal_history_retains_one_complete_journal(self):
        case = max(catalog.definitions(), key=lambda item: item.counts["http_requests"])
        self.assertEqual(case.counts["http_requests"], 70)
        self.assert_full_history_capacity(case)


    def test_epoch_descriptors_reject_duplicate_reordered_and_missing_epochs(self):
        controller, backend = self.history(restart=True)
        controller.execute_once()
        terminal = execution._json(controller.root / "terminal.json")
        original_json = execution._json
        for epochs in (terminal["epochs"] * 2, list(reversed(terminal["epochs"])), terminal["epochs"][:-1]):
            altered = dict(terminal, epochs=epochs)
            def read(path):
                return altered if path == controller.root / "terminal.json" else original_json(path)
            with self.subTest(epochs=epochs), patch.object(execution, "_json", side_effect=read):
                with self.assertRaises(ValueError):
                    controller.verified_execution()

    def test_epoch_retention_failure_preserves_cleanup_without_dangling_descriptor(self):
        controller, backend = self.history()
        original = controller._retain
        def fail_epoch(name, raw):
            if name == "step-000-epoch.json":
                raise execution.ExecutionError("inert epoch allocation exhausted")
            original(name, raw)
        with patch.object(controller, "_retain", side_effect=fail_epoch):
            result = controller.execute_once()
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.cleanup_verified)
        terminal = execution._json(controller.root / "terminal.json")
        self.assertEqual(terminal["epochs"], [])
        self.assertEqual(terminal["steps"], [])
        self.assertEqual(terminal["entered_step_indices"], [0])
        self.assertEqual(terminal["created_servers"], 1)
        self.assertEqual(terminal["attempted_probes"], 0)
        self.assertEqual(backend.probe_calls, 0)
        self.assertFalse((controller.root / "step-000-epoch.json").exists())
        before = tuple(backend.events)
        self.assertEqual(controller.execute_once().terminal_sha256, result.terminal_sha256)
        self.assertEqual(tuple(backend.events), before)


class HttpProbeDonorV3Tests(unittest.TestCase):
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
