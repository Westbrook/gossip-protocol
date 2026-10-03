"""Offline controller qualification. No candidate import, Docker or provider use."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_client_execution_v1 as execution
from gossip_harness import candidate_cli_cases_v1 as cases
from gossip_harness.gitstore import GitStore


class CandidateClientExecutionV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-client-execution-v1-offline", retain_success=True)
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
                       {"purpose": "public_release"}, {"milestone": "M4"}, {"requirements_sha256": "b" * 64}):
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
                "IpcMode": "private", "CgroupnsMode": "private"},
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
            return {"Id": keeper_id, "StartedAt": "started" if running else "created"}
        with patch.object(original, "endpoint", object()), \
             patch.object(original, "_checked", side_effect=checked), patch.object(original, "_raw", side_effect=raw), \
             patch.object(original, "_keeper_state", side_effect=keeper_state), patch.object(original, "_validate_created"), \
             patch.object(execution.process_transport, "run_process", return_value={"status": "completed"}), \
             patch.object(original, "_remove_container", side_effect=execution.ExecutionError("cleanup unproven")) as remove:
            original._dispatch(intent)
        terminal = json.loads((self.root / "terminal.json").read_bytes())
        self.assertEqual(terminal["cleanup"], {intent["containers"][0]: False})
        self.assertFalse(terminal["keeper_cleanup"])
        self.assertFalse(terminal["volume_cleanup"])
        self.assertEqual(remove.call_count, 1)
        self.assertNotIn("volume-remove", labels)

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
