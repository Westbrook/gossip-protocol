"""Offline controller qualification. No candidate import, Docker or provider use."""
from __future__ import annotations

from dataclasses import asdict, replace
import json
import socket
import threading
from copy import deepcopy
from pathlib import Path
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_client_execution_v3 as execution
from gossip_harness import candidate_cli_cases_v1 as cases
from gossip_harness.gitstore import GitStore


def inspection_fixture():
    environment = ["HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"]
    environment.extend(key + "=" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy"))
    return {"Id": "b" * 64, "Name": "/gossip-client-offline", "Created": "2026-10-03T00:00:00.001Z",
        "Image": "sha256:" + "a" * 64, "Path": "python", "Args": ["-m", "library", "list"], "RestartCount": 0,
        "Config": {"Image": "sha256:" + "a" * 64, "User": "65534:65534", "WorkingDir": "/workspace", "Tty": False,
            "OpenStdin": False, "StdinOnce": False, "AttachStdout": True, "AttachStderr": True,
            "Entrypoint": ["python"], "Cmd": ["-m", "library", "list"], "Healthcheck": {"Test": ["NONE"]},
            "Labels": {"gossip.execution": "offline", "gossip.source": "c" * 64,
                       "gossip.fixture": "d" * 64, "gossip.step": "step-1"}, "Env": environment},
        "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True, "Privileged": False,
            "OomKillDisable": False, "AutoRemove": False, "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024,
            "NanoCpus": 1000000000, "PidsLimit": 64, "CapDrop": ["ALL"], "Init": False,
            "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
            "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
            "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}], "IpcMode": "private",
            "CgroupnsMode": "private"},
        "Mounts": [{"Destination": target, "Type": "bind", "Source": str(Path(source).resolve()),
                    "RW": False, "Propagation": "rprivate"}
                   for target, source in (("/workspace", "/offline-source"), ("/inputs", "/offline-inputs"))]
                  + [{"Destination": "/tmp", "Type": "volume", "Name": "gossip-volume-offline",
                      "Driver": "local", "RW": True}],
        "State": {"Status": "created", "Running": False, "Paused": False, "Restarting": False,
            "OOMKilled": False, "Dead": False, "Pid": 0, "ExitCode": 0, "Error": "",
            "StartedAt": "0001-01-01T00:00:00Z", "FinishedAt": "0001-01-01T00:00:00Z"}}



class FakeSocket:
    def __init__(self, response, chunks=7, gate=None):
        self.gate = gate
        self.header_remaining = response.find(b"\r\n\r\n") + 4
        self.response = bytearray(response)
        self.chunk = chunks
        self.sent = []
        self.closed = False
    def settimeout(self, value):
        self.timeout = value
    def connect(self, path):
        self.path = path
    def sendall(self, raw):
        self.sent.append(raw)
    def recv(self, size, flags=0):
        if self.gate is not None and self.header_remaining == 0 and not self.gate.is_set():
            if self.timeout == 0:
                raise BlockingIOError()
            self.gate.wait(timeout=2)
        size = min(size, self.chunk)
        if self.gate is not None and self.header_remaining:
            size = min(size, self.header_remaining)
        result = bytes(self.response[:size])
        if not flags & socket.MSG_PEEK:
            del self.response[:size]
            self.header_remaining = max(0, self.header_remaining - len(result))
        return result
    def shutdown(self, how):
        pass
    def close(self):
        self.closed = True


class CandidateClientExecutionV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-client-execution-v3-offline", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git", {
            "library/__init__.py": "raise RuntimeError('candidate must never run on the host')\n",
            "library/__main__.py": "raise RuntimeError('candidate must never run on the host')\n",
        })
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)
        cls.policy = execution.ClientPolicy("sha256:" + "a" * 64)
        cls.runtime = {"kind": "fixture-no-Docker"}
        cls.case_id = cases.definitions()[0]["case_id"]

    def setUp(self):
        self.root = self.artifacts.root.resolve() / self.id().rsplit(".", 1)[-1]
        self.binding = execution.binding_for(self.files, self.case_id, self.policy, self.runtime,
                                             requirements_sha256=cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"])
        self.registration = execution.ClientRegistration(self.binding, self.commit, self.tree, "offline-qualification-1")
        self.authorities = []
        self.addCleanup(lambda: [authority.close() for authority in self.authorities])

    def authority(self, *, root=None, registration=None, checkpoint=None, sink=None):
        authority = execution.CandidateClientExecution(root or self.root, self.store,
            registration or self.registration, self.policy, mode="fixture",
            expected_checkpoint=checkpoint, checkpoint_sink=sink)
        self.authorities.append(authority)
        return authority

    def test_complete_git_source_binding_and_no_candidate_import(self):
        authority = self.authority()
        self.assertEqual(authority.files, self.files)
        self.assertEqual(authority.binding.source_sha256, execution.source_sha256(self.files))
        self.assertEqual(authority.config["source_manifest"], execution.source_manifest(self.files))
        self.assertEqual(authority.runtime, self.runtime)

    def test_only_qualification_m1_purpose_is_admitted(self):
        for change in ({"purpose": "independent_acceptance"}, {"purpose": "repeatability"},
                       {"purpose": "public_release"}, {"milestone": "M4"}, {"requirements_sha256": "b" * 64},
                       {"requirements_sha256": cases.NORMATIVE_SHA256["library-cumulative-product-v1.json"]}):
            with self.subTest(change=change), self.assertRaises(execution.ExecutionError):
                replace(self.binding, **change)

    def test_policy_rejects_boolean_unbounded_and_unpinned_inputs(self):
        for change in ({"image_id": "python:latest"}, {"command_timeout_seconds": True},
                       {"stream_limit_bytes": 0}, {"frame_limit_bytes": 4 * 1024 * 1024 + 1},
                       {"transport_timeout_seconds": 61}, {"seed": -1}):
            with self.subTest(change=change), self.assertRaises(execution.ExecutionError):
                replace(self.policy, **change)

        endpoint = object()
        with patch.object(execution.process_transport, "runtime_identity", return_value={"runtime": "controlled"}) as runtime:
            self.assertEqual(execution.runtime_identity(endpoint, self.policy.image_id, timeout_seconds=11), {"runtime": "controlled"})
        runtime.assert_called_once_with(endpoint, self.policy.image_id, timeout_seconds=11)

    def test_every_closed_recipe_fits_prospective_fixture_step_and_argument_bounds(self):
        count = 0
        for definition in cases.definitions():
            recipe, files = execution.recipe_for(definition["case_id"])
            self.assertEqual(recipe["case_id"], definition["case_id"])
            self.assertLessEqual(sum(map(len, files.values())), execution.MAX_FIXTURE_BYTES)
            self.assertEqual(set(recipe), {"case_id", "fixtures", "directories", "steps"})
            for step in recipe["steps"]:
                self.assertEqual(set(step), {"step_id", "argv"})
            count += len(recipe["steps"])
        self.assertEqual(len(cases.definitions()), 57)
        self.assertEqual(count, 305)

    def test_recipe_rejects_path_collisions_traversal_noncanonical_bytes_and_duplicate_steps(self):
        original = cases.execution_recipe(self.case_id)
        bad_values = []
        for fixtures, directories in (({"../foreign": ""}, []), ({"a": "", "a/b": ""}, []),
                                      ({"a": ""}, ["a"]), ({"a": "YQ==\n"}, [])):
            bad_values.append(dict(original, fixtures=fixtures, directories=directories))
        bad_values.append(dict(original, steps=[original["steps"][0], original["steps"][0]]))
        bad_values.append(dict(original, steps=[{"step_id": "x", "argv": ["python", "a\x00b"]}]))
        for recipe in bad_values:
            with self.subTest(recipe=recipe), patch.object(cases, "execution_recipe", return_value=recipe):
                with self.assertRaises(execution.ExecutionError):
                    execution.recipe_for(self.case_id)

    def test_changed_source_recipe_suite_runtime_evaluator_and_limits_do_not_register(self):
        keys = ("source_sha256", "definition_sha256", "ordered_suite_sha256", "runtime_sha256",
                "environment_sha256", "evaluator_sha256", "limits_sha256", "seed_sha256")
        for index, key in enumerate(keys):
            changed = replace(self.registration, binding=replace(self.binding, **{key: "0" * 64}))
            with self.subTest(key=key), self.assertRaises(execution.ExecutionError):
                self.authority(root=self.root.with_name(self.root.name + str(index)), registration=changed)

    def test_existing_root_requires_exact_external_checkpoint_and_reopens_without_execution(self):
        original = self.authority()
        checkpoint = original.checkpoint()
        original.close()
        with self.assertRaises(execution.ExecutionError):
            self.authority()
        reopened = self.authority(checkpoint=checkpoint)
        self.assertEqual(reopened.checkpoint(), checkpoint)
        self.assertFalse((self.root / "intent.json").exists())

    def test_active_owner_excludes_second_controller(self):
        original = self.authority()
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=original.checkpoint())

    def test_checkpoint_rejects_appended_changed_and_missing_journal_files(self):
        original = self.authority()
        checkpoint = original.checkpoint()
        original.close()
        config = self.root / "config.json"
        raw = config.read_bytes()
        (self.root / "foreign.json").write_text("{}")
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=checkpoint)
        (self.root / "foreign.json").unlink()
        config.write_bytes(raw + b" ")
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=checkpoint)
        config.unlink()
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=checkpoint)

    def test_checkpoint_sink_runs_after_fsynced_creation(self):
        receipts = []
        original = self.authority(sink=lambda checkpoint: receipts.append(checkpoint))
        self.assertEqual(receipts, [original.checkpoint()])
        self.assertTrue((self.root / "config.json").is_file())

    def test_unknown_intent_is_retained_and_never_redispatched(self):
        original = self.authority()
        with self.assertRaises(execution.ExecutionError):
            original.execute_once()
        intent = (self.root / "intent.json").read_bytes()
        checkpoint = original.checkpoint()
        original.close()
        reopened = self.authority(checkpoint=checkpoint)
        with patch.object(reopened, "_dispatch", side_effect=AssertionError("redispatch")):
            with self.assertRaises(execution.ExecutionUnknown):
                reopened.execute_once()
        self.assertEqual((self.root / "intent.json").read_bytes(), intent)

    def test_fixture_journal_cannot_authenticate_physical_execution(self):
        original = self.authority()
        with self.assertRaises(execution.ExecutionError):
            original.verified_execution()

    def test_changed_loaded_evaluator_or_normative_source_fails_before_dispatch(self):
        original = self.authority()
        with patch.object(execution, "evaluator_sources", return_value={"changed": "0" * 64}):
            with self.assertRaises(execution.ExecutionError):
                original.execute_once()
        with patch.object(cases, "definition_sources", return_value={"changed": "0" * 64}):
            with self.assertRaises(execution.ExecutionError):
                original.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_close_disables_controller_capability(self):
        original = self.authority()
        original.close()
        with self.assertRaises(execution.ExecutionError):
            original.checkpoint()

    def test_create_argv_uses_exact_main_process_and_only_source_inputs_volume_mounts(self):
        original = self.authority()
        step = original.recipe["steps"][0]
        argv = original._create_argv("gossip-test", Path("/safe/workspace"), Path("/safe/inputs"),
                                     "gossip-volume-test", step, "client-test")
        self.assertEqual(argv[:2], ["docker", "create"])
        self.assertNotIn("--init", argv)
        self.assertNotIn("--rm", argv)
        self.assertIn("--network=none", argv)
        self.assertIn("--read-only", argv)
        self.assertIn("--user=65534:65534", argv)
        self.assertIn("--log-driver=none", argv)
        self.assertIn("type=volume,source=gossip-volume-test,target=/tmp,volume-nocopy", argv)
        self.assertTrue(any("target=/workspace,readonly" in arg for arg in argv))
        self.assertTrue(any("target=/inputs,readonly" in arg for arg in argv))
        self.assertFalse(any("target=/checks" in arg for arg in argv))
        self.assertEqual(argv[argv.index(self.policy.image_id) + 1:], step["argv"][1:])

    def test_created_container_matches_registered_argv_labels_and_staging_before_start(self):
        original = self.authority()
        step = original.recipe["steps"][0]
        intent = {"execution_id": "client-test", "volume": "gossip-volume"}
        valid = {"Id": "d" * 64, "Name": "/gossip-candidate", "Image": self.policy.image_id,
            "Path": step["argv"][0], "Args": step["argv"][1:],
            "Config": {"Labels": {"gossip.execution": "client-test", "gossip.source": self.binding.source_sha256,
                "gossip.fixture": original.config["fixture_sha256"], "gossip.step": step["step_id"]}},
            "Mounts": [{"Destination": "/workspace", "Source": "/safe/source"},
                {"Destination": "/inputs", "Source": "/safe/inputs"}, {"Destination": "/tmp", "Name": "gossip-volume"}]}
        def check(value):
            original._validate_created(value, container_id="d" * 64, name="gossip-candidate", workspace=Path("/safe/source"),
                                       inputs=Path("/safe/inputs"), intent=intent, step=step)
        with patch.object(execution.process_transport, "validate_sandbox") as generic:
            check(valid)
            generic.assert_called_once()
        bad = [dict(valid, Args=["wrong"]), dict(valid, Config={"Labels": dict(valid["Config"]["Labels"], **{"gossip.source": "0" * 64})}),
               dict(valid, Mounts=[dict(valid["Mounts"][0], Source="/foreign"), *valid["Mounts"][1:]]),
               dict(valid, Mounts=[*valid["Mounts"][:2], dict(valid["Mounts"][2], Name="foreign")])]
        for value in bad:
            with self.subTest(value=value), patch.object(execution.process_transport, "validate_sandbox") as generic:
                with self.assertRaises(execution.ExecutionError):
                    check(value)
                generic.assert_not_called()

    def test_keeper_has_bounded_lifetime_readonly_volume_and_no_candidate_mount(self):
        original = self.authority()
        intent = {"volume": "gossip-volume-test", "execution_id": "client-test"}
        argv = original._keeper_create_argv("gossip-keeper", Path("/safe/source"), Path("/safe/inputs"), intent)
        self.assertEqual(argv[:2], ["docker", "create"])
        self.assertEqual(argv.count("--mount"), 1)
        self.assertIn("type=volume,source=gossip-volume-test,target=/tmp,readonly,volume-nocopy", argv)
        self.assertFalse(any("source=/safe" in arg for arg in argv))
        self.assertIn("--workdir=/", argv)
        self.assertNotIn("--init", argv)
        self.assertNotIn("--rm", argv)
        lifetime = execution.keeper_lifetime_seconds(original.recipe, original.policy)
        self.assertGreater(lifetime, len(original.recipe["steps"]) * original.policy.command_timeout_seconds)
        self.assertEqual(argv[-1], "import time;time.sleep(" + str(lifetime) + ")")

    def test_keeper_restart_death_and_writable_or_foreign_mount_are_unavailable(self):
        original = self.authority()
        container_id = "d" * 64
        intent = {"keeper": "gossip-keeper", "volume": "gossip-volume", "execution_id": "client-test",
                  "keeper_argv": list(execution.keeper_command(original.recipe, original.policy))}
        valid = {"Id": container_id, "Name": "/gossip-keeper", "Image": self.policy.image_id,
            "Path": intent["keeper_argv"][0], "Args": intent["keeper_argv"][1:], "RestartCount": 0,
            "Config": {"Image": self.policy.image_id, "User": "65534:65534", "WorkingDir": "/",
                "Entrypoint": intent["keeper_argv"][:1], "Cmd": intent["keeper_argv"][1:],
                "Tty": False, "OpenStdin": False, "StdinOnce": False, "Healthcheck": {"Test": ["NONE"]},
                "Labels": {"gossip.execution": "client-test", "gossip.role": "volume-keeper"},
                "Env": ["HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"] + [key + "=" for key in
                    ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")]},
            "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True, "Privileged": False, "Init": False, "AutoRemove": False,
                "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024, "NanoCpus": 1000000000, "PidsLimit": 64,
                "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
                "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0}, "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}],
                "IpcMode": "private", "CgroupnsMode": "private", "OomKillDisable": False},
            "Mounts": [{"Type": "volume", "Driver": "local", "Name": "gossip-volume", "Destination": "/tmp", "RW": False}],
            "State": {"Status": "running", "Running": True, "Paused": False, "Restarting": False, "Dead": False, "OOMKilled": False,
                      "Error": "", "StartedAt": "2026-10-03T01:02:03.123Z"}}
        with patch.object(original, "_checked", return_value={}), patch.object(original, "_raw", return_value=json.dumps(valid).encode()):
            identity = original._keeper_state("keeper", intent, container_id)
        self.assertEqual(identity["Id"], container_id)
        bad = [dict(valid, RestartCount=1), dict(valid, State=dict(valid["State"], Running=False)),
               dict(valid, Mounts=[dict(valid["Mounts"][0], RW=True)]),
               dict(valid, Mounts=[dict(valid["Mounts"][0], Name="foreign")]),
               dict(valid, RestartCount=False), dict(valid, HostConfig=dict(valid["HostConfig"], CapDrop=[])),
               dict(valid, HostConfig=dict(valid["HostConfig"], RestartPolicy={"Name": "always", "MaximumRetryCount": 0}))]
        for value in bad:
            with self.subTest(value=value), patch.object(original, "_checked", return_value={}), patch.object(original, "_raw", return_value=json.dumps(value).encode()):
                with self.assertRaises(execution.ExecutionError):
                    original._keeper_state("keeper", intent, container_id)

        for bad_value in (True, 0, 1, "false", [], {}):
            changed = deepcopy(valid)
            changed["HostConfig"]["OomKillDisable"] = bad_value
            with self.subTest(oom_value=bad_value), patch.object(original, "_checked", return_value={}), \
                 patch.object(original, "_raw", return_value=json.dumps(changed).encode()):
                with self.assertRaises(ValueError):
                    original._keeper_state("keeper", intent, container_id)
        absent = deepcopy(valid)
        del absent["HostConfig"]["OomKillDisable"]
        with patch.object(original, "_checked", return_value={}), patch.object(original, "_raw", return_value=json.dumps(absent).encode()):
            with self.assertRaises(ValueError):
                original._keeper_state("keeper", intent, container_id)

    def test_startup_policy_is_bound_in_configuration_and_limits(self):
        original = self.authority()
        policy = execution.startup_policy_binding()
        self.assertEqual(original.config["startup_compatibility_policy"], policy)
        self.assertEqual(policy["policy_id"], "docker-hostconfig-oom-kill-default-v1")
        self.assertEqual(policy["policy_sha256"], execution.process_transport.startup_policy_sha256())
        self.assertIn(execution.COMPATIBILITY_PLAN, original.sources)
        changed = dict(policy, policy_sha256="0" * 64)
        with patch.object(execution, "startup_policy_binding", return_value=changed):
            different = execution.binding_for(self.files, self.case_id, self.policy, self.runtime,
                requirements_sha256=cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"])
        self.assertNotEqual(different.limits_sha256, original.binding.limits_sha256)

    def test_raw_keeper_boundaries_never_reuse_startup_normalization(self):
        running = {"Id": "d" * 64, "HostConfig": {"OomKillDisable": None, "Unlisted": False},
                   "StartedAt": "2026-10-03T01:02:03.123Z", "RestartCount": 0,
                   "Mounts": [{"Name": "owned-volume", "RW": False}]}
        self.assertTrue(execution.exact_identity_equal(running, deepcopy(running)))
        for path, value in (("OomKillDisable", False), ("Unlisted", 0)):
            changed = deepcopy(running)
            changed["HostConfig"][path] = value
            self.assertFalse(execution.exact_identity_equal(running, changed))
        for changed in (dict(running, StartedAt="2026-10-03T01:02:04.123Z"),
                        dict(running, RestartCount=False), dict(running, RestartCount=1),
                        dict(running, Mounts=[{"Name": "foreign-volume", "RW": False}])):
            self.assertFalse(execution.exact_identity_equal(running, changed))

    def test_keeper_initial_startup_comparison_preserves_raw_values_and_all_other_fields(self):
        runtime = {"protocol": "candidate-client-process-v3", "endpoint": {"socket_path": "/fixture/docker.sock", "device": 1, "inode": 2},
            "api_version": "1.47", "daemon_id": "fixture-daemon", "engine_version": "29.2.1",
            "engine_git_commit": "6bc6209", "os": "linux", "architecture": "arm64", "kernel_version": "fixture-kernel",
            "image_id": self.policy.image_id, "image_inspect_sha256": "e" * 64, "cgroup_version": "2",
            "cgroup_driver": "cgroupfs", "oom_kill_disable_supported": False}
        before = {"Id": "d" * 64, "Image": self.policy.image_id, "Name": "/gossip-keeper",
                  "Path": "python", "Args": ["-I", "-c", "import time;time.sleep(120)"],
                  "HostConfig": {"OomKillDisable": False, "Unlisted": False},
                  "Config": {"Labels": {"gossip.role": "volume-keeper"}}, "RestartCount": 0,
                  "Mounts": [{"Name": "owned-volume", "Destination": "/tmp", "RW": False}]}
        after = deepcopy(before)
        after["HostConfig"]["OomKillDisable"] = None
        original_bytes = execution.encoded(before), execution.encoded(after)
        result = execution.process_transport.startup_identity_comparison(before, after, runtime,
            phase="keeper-created-to-running")
        self.assertTrue(result["matches"], result)
        self.assertEqual(len(result["transformations"]), 1)
        self.assertEqual((execution.encoded(before), execution.encoded(after)), original_bytes)
        for mutation in ("Unlisted", "Id", "RestartCount"):
            changed = deepcopy(after)
            if mutation == "Unlisted":
                changed["HostConfig"]["Unlisted"] = 0
            elif mutation == "Id":
                changed["Id"] = "c" * 64
            else:
                changed["RestartCount"] = False
            rejected = execution.process_transport.startup_identity_comparison(before, changed, runtime,
                phase="keeper-created-to-running")
            self.assertFalse(rejected["matches"], mutation)
        for changed_runtime in (dict(runtime, oom_kill_disable_supported=True), dict(runtime, cgroup_version="1")):
            rejected = execution.process_transport.startup_identity_comparison(before, after, changed_runtime,
                phase="keeper-created-to-running")
            self.assertFalse(rejected["matches"])

    def test_cleanup_refuses_foreign_container_even_with_requested_name(self):
        original = self.authority()
        step = original.recipe["steps"][0]
        value = {"Id": "c" * 64, "Name": "/gossip-owned", "Image": self.policy.image_id,
                 "Config": {"Labels": {"gossip.execution": "foreign"}}}
        clean = {"exit_code": 0, "timed_out": False, "capture_complete": True,
                 "stdout": {"truncated": False}, "stderr": {"truncated": False}}
        calls = []
        def command(label, argv):
            calls.append(argv)
            return clean
        with patch.object(original, "_command", side_effect=command), patch.object(original, "_raw", return_value=json.dumps(value).encode()):
            with self.assertRaises(execution.ExecutionError):
                original._remove_container("step-000", "gossip-owned", {"execution_id": "client-owned"}, step, None)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("rm", calls[0])

    def test_cleanup_exception_blocks_keeper_and_volume_removal(self):
        original = self.authority()
        with self.assertRaises(execution.ExecutionError):
            original.execute_once()  # Fixture intent retained; dispatch is explicitly mocked below.
        intent = json.loads((self.root / "intent.json").read_bytes())
        candidate_id, keeper_id = "c" * 64, "d" * 64
        labels = []
        def checked(label, argv):
            labels.append(label)
            return {"label": label}
        def raw(record, kind="stdout"):
            label = record["label"]
            if label in ("volume-before", "keeper-before", "step-000-before"):
                return b""
            if label == "volume-create":
                return intent["volume"].encode()
            if label in ("volume-created", "volume-cleanup-inspect"):
                return execution.encoded({"Name": intent["volume"], "Driver": "local", "Options": execution.VOLUME_OPTIONS,
                    "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}})
            if label == "keeper-create":
                return keeper_id.encode()
            if label == "step-000-create":
                return candidate_id.encode()
            if label == "step-000-created":
                return b"{}"
            raise AssertionError("Unexpected control observation " + label)
        def keeper_state(label, intent, keeper_id, *, running=True):
            original._keeper_inspection = {"kind": "offline-mocked-full-inspection"}
            return {"Id": keeper_id, "StartedAt": "started" if running else "created"}
        def process(endpoint, *, container_id, expected, policy, retain, label, expected_runtime):
            retain(label + "-intent.json", execution.encoded({"kind": "explicit-offline-process-mock"}))
            return {"status": "completed"}
        with patch.object(original, "endpoint", object()), \
             patch.object(original, "_checked", side_effect=checked), patch.object(original, "_raw", side_effect=raw), \
             patch.object(original, "_keeper_state", side_effect=keeper_state), patch.object(original, "_validate_created"), \
             patch.object(execution.process_transport, "run_process", side_effect=process), \
             patch.object(execution.process_transport, "identity_comparison", return_value={"matches": True}), \
             patch.object(original, "_remove_container", side_effect=execution.ExecutionError("cleanup unproven")) as remove:
            original._dispatch(intent)
        terminal = json.loads((self.root / "terminal.json").read_bytes())
        self.assertEqual(terminal["cleanup"], {intent["containers"][0]: False})
        self.assertFalse(terminal["keeper_cleanup"])
        self.assertFalse(terminal["volume_cleanup"])
        self.assertEqual(remove.call_count, 1)
        self.assertEqual(len(terminal["results"]), 1)
        self.assertTrue((self.root / "step-000-controller-intent.json").is_file())
        self.assertEqual(json.loads((self.root / "step-000-intent.json").read_bytes()), {"kind": "explicit-offline-process-mock"})
        self.assertNotIn("volume-remove", labels)

    def test_inventory_policy_and_both_plans_are_bound_in_configuration_and_limits(self):
        original = self.authority()
        policy = execution.identity_policy_binding()
        self.assertEqual(original.config["identity_comparison_policy"], policy)
        self.assertEqual(policy["policy_id"], "docker-inspect-mount-inventory-v1")
        self.assertEqual(policy["policy_sha256"], execution.process_transport.identity_policy_sha256())
        for plan in (execution.COMPATIBILITY_PLAN, execution.MOUNT_INVENTORY_PLAN):
            self.assertIn(plan, original.sources)
            self.assertEqual(original.sources[plan], execution.sha256((Path(execution.__file__).resolve().parents[1] / plan).read_bytes()))
        with patch.object(execution, "identity_policy_binding", return_value=dict(policy, policy_sha256="0" * 64)):
            changed = execution.binding_for(self.files, self.case_id, self.policy, self.runtime,
                requirements_sha256=cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"])
        self.assertNotEqual(changed.limits_sha256, original.binding.limits_sha256)

    def test_duplicate_keys_in_raw_keeper_and_cleanup_inspections_never_become_mappings(self):
        original = self.authority()
        duplicate_views = (b'{"Mounts":[],"Mounts":[]}',
                           b'{"Mounts":[{"Destination":"/tmp","Destination":"/tmp"}]}',
                           b'{"Mounts":[{"Destination":"/tmp","Meta":{"x":false,"x":0}}]}')
        clean = {"exit_code": 0, "timed_out": False, "capture_complete": True,
                 "stdout": {"truncated": False}, "stderr": {"truncated": False}}
        for raw in duplicate_views:
            with self.subTest(raw=raw), patch.object(original, "_checked", return_value=clean), \
                 patch.object(original, "_raw", return_value=raw):
                with self.assertRaisesRegex(execution.process_transport.ProcessError, "Duplicate"):
                    original._keeper_state("keeper", {}, "d" * 64)
            with self.subTest(cleanup_raw=raw), patch.object(original, "_command", return_value=clean) as command, \
                 patch.object(original, "_raw", return_value=raw):
                with self.assertRaisesRegex(execution.process_transport.ProcessError, "Duplicate"):
                    original._remove_container("step-000", "gossip-owned", {}, original.recipe["steps"][0], None)
                self.assertEqual(command.call_count, 1)

    def mocked_controller_transport(self, *, mutation=False, duplicate=None, keeper_change=False):
        """Actual dispatch, parsing, identity, lifecycle and journal; only I/O is inert."""
        original = self.authority()
        with self.assertRaises(execution.ExecutionError):
            original.execute_once()  # Fixture mode retains intent but prohibits actual dispatch.
        intent = json.loads((self.root / "intent.json").read_bytes())
        process = execution.process_transport
        endpoint = process.EngineEndpoint("/never-opened-offline.sock", 1, 2)
        runtime = {"protocol": process.PROTOCOL, "endpoint": asdict(endpoint), "api_version": "1.47",
            "daemon_id": "offline-daemon", "engine_version": "29.2.1", "os": "linux", "engine_git_commit": "6bc6209",
            "architecture": "arm64", "kernel_version": "offline-kernel", "image_id": self.policy.image_id,
            "image_inspect_sha256": "e" * 64, "cgroup_version": "2", "cgroup_driver": "cgroupfs",
            "oom_kill_disable_supported": False}
        views, events, state = {}, [], {}
        keeper_id = "d" * 64
        gate = threading.Event()
        retained = original._retain
        def retain(name, raw):
            retained(name, raw)
            events.append(name)
        def command(label, argv):
            events.append("cli:" + label)
            raw = b""
            if label == "volume-create":
                raw = intent["volume"].encode()
            elif label in ("volume-created", "volume-cleanup-inspect"):
                raw = execution.encoded({"Name": intent["volume"], "Driver": "local", "Options": execution.VOLUME_OPTIONS,
                    "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}})
            elif label == "keeper-create" or (label.startswith("step-") and label.endswith("-create")):
                keeper = label == "keeper-create"
                index = 0 if keeper else int(label.split("-")[1])
                container_id = keeper_id if keeper else format(index + 1, "064x")
                name = intent["keeper"] if keeper else intent["containers"][index]
                literal = intent["keeper_argv"] if keeper else original.recipe["steps"][index]["argv"]
                value = inspection_fixture()
                value.update(Id=container_id, Name="/" + name, Path=literal[0], Args=literal[1:])
                value["Config"].update(Entrypoint=literal[:1], Cmd=literal[1:], WorkingDir="/" if keeper else "/workspace")
                value["Config"]["Labels"] = ({"gossip.execution": intent["execution_id"], "gossip.role": "volume-keeper"} if keeper else
                    {"gossip.execution": intent["execution_id"], "gossip.source": original.binding.source_sha256,
                     "gossip.fixture": original.config["fixture_sha256"], "gossip.step": original.recipe["steps"][index]["step_id"]})
                mounts = []
                for pos, part in enumerate(argv):
                    if part != "--mount":
                        continue
                    mount = dict(item.split("=", 1) if "=" in item else (item, True) for item in argv[pos + 1].split(","))
                    if mount["type"] == "volume":
                        mounts.append({"Destination": mount["target"], "Type": "volume", "Driver": "local", "Name": mount["source"], "RW": not keeper})
                    else:
                        mounts.append({"Destination": mount["target"], "Type": "bind", "Source": mount["source"], "RW": False, "Propagation": "rprivate"})
                value["Mounts"] = mounts
                views[container_id] = value
                if not keeper:
                    state["candidate_id"] = container_id
                    state["label"] = label.removesuffix("-create")
                    gate.clear()
                raw = container_id.encode()
            elif label == "keeper-start":
                views[keeper_id]["HostConfig"]["OomKillDisable"] = None
                views[keeper_id]["State"].update(Status="running", Running=True, Pid=42, StartedAt="2026-10-03T00:00:01.000000001Z")
            elif "inspect" in argv:
                selected = argv[-1]
                value = views[selected] if selected in views else next(v for v in views.values() if v["Name"] == "/" + selected)
                value = deepcopy(value)
                if keeper_change and label == "step-000-keeper-after":
                    value["HostConfig"]["OomKillDisable"] = False
                raw = execution.encoded(value)
                if duplicate and label == "step-000-created":
                    raw = raw.replace(b'"Mounts":', b'"Mounts":[],"Mounts":', 1)
            record = {"argv": argv, "exit_code": 0, "timed_out": False, "capture_complete": True}
            for stream, data in (("stdout", raw), ("stderr", b"")):
                path = label + "-" + stream + ".bin"
                original._retain(path, data)
                record[stream] = {"path": path, "sha256": execution.sha256(data), "bytes": len(data), "observed_bytes": len(data), "truncated": False}
            original._retain(label + ".json", execution.encoded(record))
            return record
        def json_control(endpoint, path, **kwargs):
            label = kwargs["label"]
            events.append("engine:" + label)
            if label == "wait":
                gate.set()
                return {"StatusCode": 0}
            value = deepcopy(views[state["candidate_id"]])
            if label == "inspect-before":
                value["Mounts"] = value["Mounts"][1:] + value["Mounts"][:1]
                if mutation:
                    value["Mounts"][0]["Source"] = "/foreign-substitution"
            elif label == "inspect-final":
                value["Mounts"] = value["Mounts"][-1:] + value["Mounts"][:-1]
                value["HostConfig"]["OomKillDisable"] = None
                value["State"].update(Status="exited", StartedAt="2026-10-03T00:00:02.000000001Z", FinishedAt="2026-10-03T00:00:02.000000002Z")
            return value
        def control(endpoint, method, path, **kwargs):
            label = kwargs["label"]
            events.append("engine:" + label)
            if label == "start":
                prefix = state["label"]
                for artifact in (prefix + "-controller-intent.json", prefix + "-intent.json", prefix + "-prestart-comparison.json", prefix + "-attach-request.bin"):
                    self.assertIn(artifact, events)
                comparison = json.loads((self.root / (prefix + "-prestart-comparison.json")).read_bytes())
                self.assertTrue(comparison["matches"])
                kwargs["on_response"](204)
            return 204, b""
        def socket_factory(*_args, **_kwargs):
            events.append("engine:attach-socket")
            raw = b"output\x00\xff"
            frame = b"\x01\x00\x00\x00" + len(raw).to_bytes(4, "big") + raw
            return FakeSocket(b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\n\r\n" + frame, gate=gate)
        with patch.object(original, "endpoint", endpoint), patch.object(original, "runtime", runtime), \
             patch.object(original, "_command", side_effect=command), patch.object(original, "_retain", side_effect=retain), \
             patch.object(process, "runtime_identity", return_value=runtime), patch.object(process.EngineEndpoint, "validate"), \
             patch.object(process, "_json_control", side_effect=json_control), patch.object(process, "_control", side_effect=control), \
             patch.object(process.socket, "socket", side_effect=socket_factory):
            original._dispatch(intent)
        return json.loads((self.root / "terminal.json").read_bytes()), events

    def test_controller_to_real_transport_retains_ordered_views_before_start(self):
        terminal, events = self.mocked_controller_transport()
        self.assertEqual(terminal["infrastructure"], [])
        self.assertEqual(len(terminal["results"]), len(cases.execution_recipe(self.case_id)["steps"]))
        self.assertTrue(terminal["keeper_cleanup"] and terminal["volume_cleanup"])
        for row in terminal["results"]:
            self.assertEqual(row["result"]["status"], "completed")
            self.assertTrue(row["result"]["history_state_verified"])
            prefix = row["label"]
            for suffix in ("prestart-comparison", "startup-comparison", "keeper-before-comparison", "keeper-after-comparison"):
                proof = json.loads((self.root / (prefix + "-" + suffix + ".json")).read_bytes())
                self.assertTrue(proof["matches"], proof)
                self.assertTrue(proof["before_full_inspection_sha256"] and proof["after_full_inspection_sha256"])
                self.assertEqual(proof["mounts_before"]["inventory_sha256"], proof["mounts_after"]["inventory_sha256"])
                if not suffix.startswith("keeper"):
                    self.assertNotEqual(proof["mounts_before"]["order"], proof["mounts_after"]["order"])
                    self.assertNotEqual(proof["before_sha256"], proof["after_sha256"])
            self.assertLess(events.index(prefix + "-prestart-comparison.json"), events.index(prefix + "-attach-request.bin"))
        self.assertEqual(events.count("engine:start"), len(terminal["results"]))

    def test_controller_to_real_transport_rejects_actual_mount_change_before_attach(self):
        terminal, events = self.mocked_controller_transport(mutation=True)
        self.assertEqual(len(terminal["results"]), 1)
        result = terminal["results"][0]["result"]
        self.assertEqual(result["status"], "transport_error")
        self.assertFalse(result["completion"]["started"])
        self.assertNotIn("engine:start", events)
        self.assertNotIn("engine:attach-socket", events)
        self.assertTrue(terminal["keeper_cleanup"] and terminal["volume_cleanup"])
        proof = json.loads((self.root / "step-000-prestart-comparison.json").read_bytes())
        self.assertFalse(proof["matches"])

    def test_controller_running_keeper_boundary_refuses_repeated_startup_normalization(self):
        terminal, events = self.mocked_controller_transport(keeper_change=True)
        self.assertEqual(len(terminal["results"]), 1)
        result = terminal["results"][0]["result"]
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["completion"]["natural"])
        self.assertFalse(result["history_state_verified"])
        self.assertTrue(any("Keeper lifetime" in error for error in terminal["infrastructure"]))
        proof = json.loads((self.root / "step-000-keeper-after-comparison.json").read_bytes())
        self.assertEqual(proof["phase"], "keeper-running-to-running")
        self.assertFalse(proof["matches"])
        self.assertEqual(proof["transformations"], [])
        self.assertEqual(events.count("engine:start"), 1)
        self.assertTrue(terminal["keeper_cleanup"] and terminal["volume_cleanup"])

    def test_raw_created_cli_duplicate_keys_block_real_transport_entry(self):
        terminal, events = self.mocked_controller_transport(duplicate="candidate")
        self.assertEqual(terminal["results"], [])
        self.assertTrue(any("Duplicate" in error for error in terminal["infrastructure"]))
        self.assertNotIn("engine:inspect-before", events)
        self.assertNotIn("engine:start", events)
        self.assertTrue(terminal["keeper_cleanup"] and terminal["volume_cleanup"])

    def test_staged_source_and_empty_directory_census_detects_substitution(self):
        stage = self.root
        stage.mkdir()
        (stage / "root-a").mkdir()
        (stage / "root-a" / "input.txt").write_bytes(b"one")
        execution._verify_tree(stage, {"root-a/input.txt": b"one"}, ["root-a"])
        (stage / "root-b").mkdir()
        with self.assertRaises(execution.ExecutionError):
            execution._verify_tree(stage, {"root-a/input.txt": b"one"}, ["root-a"])
        (stage / "root-b").rmdir()
        (stage / "root-a" / "input.txt").write_bytes(b"two")
        with self.assertRaises(execution.ExecutionError):
            execution._verify_tree(stage, {"root-a/input.txt": b"one"}, ["root-a"])

    def test_step_binding_carries_exact_argv_order_source_and_full_suite(self):
        original = self.authority()
        bound = original._step_binding({"execution_id": "client-test"}, 0)
        self.assertEqual(bound["ordered_suite_sha256"], self.binding.ordered_suite_sha256)
        self.assertEqual(bound["source_sha256"], self.binding.source_sha256)
        self.assertEqual(bound["argv"], original.recipe["steps"][0]["argv"])
        self.assertEqual(bound["ordered_step_ids"], [step["step_id"] for step in original.recipe["steps"]])
        self.assertEqual(bound["purpose"], "harness_qualification")


if __name__ == "__main__":
    unittest.main()
