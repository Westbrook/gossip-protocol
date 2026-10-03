"""Controller journal qualification with explicit mocks, never study evidence."""
from __future__ import annotations

from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_release_execution_v2 as execution
from gossip_harness import candidate_release_observer_v2 as observer
from gossip_harness import candidate_release_execution_v1 as legacy_execution
from gossip_harness import candidate_release_observer_v1 as legacy_observer
from gossip_harness.gitstore import GitStore, _run
from gossip_harness.project_acceptance_registry_v1 import CohortFreeze, Gate, Subject

TRAJECTORIES = ("s4-healthy", "s4-fault", "s16-healthy", "s16-fault", "o16-healthy", "o16-fault")


def make_store(path: Path, files: dict[str, bytes]) -> GitStore:
    texts = {}
    for name, raw in files.items():
        try:
            texts[name] = raw.decode("utf-8")
        except UnicodeError:
            pass
    store = GitStore.create(path, texts)
    if len(texts) != len(files):
        with store._checkout(store.head()) as checkout:
            for name, raw in files.items():
                target = checkout / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
            _run(checkout, "add", "--all")
            _run(checkout, "commit", "-m", "Add frozen binary inputs")
            oid = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
            store._git("fetch", "--no-tags", str(checkout), oid + ":refs/heads/accepted")
    return store


def registration(store: GitStore, policy: execution.ReleasePolicy, runtime: dict, *, purpose="public_release"):
    commit = store.head()
    tree, files = execution.capture_git_source(store, commit)
    subject = Subject("qualified-cohort", TRAJECTORIES[0], "M4", "a" * 64,
                      observer.PRODUCT_CONTRACT_SHA256, execution.source_sha256(files))
    binding = execution.binding_for(subject, policy, runtime, purpose=purpose)
    gate = Gate("packaged-release-smoke", ("M4-RELEASE-HANDOFF",), observer.CASE_IDS, binding)
    return execution.Registration(gate, commit, tree, "declared-repetition-1", TRAJECTORIES)


class CandidateReleaseV2ExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-release-v2-journal", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.files = {"library/__init__.py": b"# fixture, not product execution\n", "public/snapshot.bin": b"\xff\x00\xfe"}
        cls.store = make_store(cls.artifacts.root / "store.git", cls.files)
        cls.policy = execution.ReleasePolicy(observer.RUNTIME_IMAGE)
        cls.runtime = {"kind": "fixture-no-Docker"}

    def setUp(self):
        self.root = self.artifacts.root.resolve() / self.id().rsplit(".", 1)[-1]
        self.reg = registration(self.store, self.policy, self.runtime)
        self.authorities = []
        self.addCleanup(lambda: [authority.close() for authority in self.authorities])

    def authority(self, *, root=None, reg=None, checkpoint=None, freeze=None):
        authority = execution.CandidateReleaseExecution(root or self.root, self.store, reg or self.reg, self.policy,
            mode="fixture", expected_checkpoint=checkpoint, verify_cohort=freeze)
        self.authorities.append(authority)
        return authority

    def test_binary_git_capture_and_exact_binding(self):
        tree, files = execution.capture_git_source(self.store, self.store.head())
        self.assertEqual(files, self.files)
        self.assertEqual(tree, self.reg.tree_oid)
        authority = self.authority()
        self.assertEqual(authority.binding, self.reg.gate.binding)
        self.assertTrue(authority.checkpoint().files)

    def test_mismatched_source_suite_order_purpose_and_evaluator_rejected(self):
        for index, binding in enumerate((replace(self.reg.gate.binding, subject=replace(self.reg.gate.binding.subject,
                    source_sha256="0" * 64)), replace(self.reg.gate.binding, ordered_suite_sha256="0" * 64),
                    replace(self.reg.gate.binding, evaluator_sha256="0" * 64),
                    replace(self.reg.gate.binding, limits_sha256="0" * 64))):
            with self.subTest(index=index), self.assertRaises(execution.ExecutionError):
                self.authority(root=self.root.with_name(self.root.name + str(index)),
                               reg=replace(self.reg, gate=replace(self.reg.gate, binding=binding)))
        with self.assertRaises(execution.ExecutionError):
            self.authority(root=self.root.with_name(self.root.name + "order"),
                reg=replace(self.reg, gate=replace(self.reg.gate, ordered_case_ids=tuple(reversed(observer.CASE_IDS)))))
        authority = self.authority()
        checkpoint = authority.checkpoint()
        authority.close()
        changed = replace(self.reg, gate=replace(self.reg.gate, binding=replace(self.reg.gate.binding,
                                                                               purpose="repeatability")))
        with self.assertRaises(execution.ExecutionError):
            self.authority(reg=changed, checkpoint=checkpoint)

    def test_external_exact_checkpoint_required_and_foreign_suffix_rejected(self):
        authority = self.authority()
        checkpoint = authority.checkpoint()
        authority.close()
        with self.assertRaises(execution.ExecutionError):
            self.authority()
        resumed = self.authority(checkpoint=checkpoint)
        self.assertEqual(resumed.checkpoint(), checkpoint)
        resumed.close()
        (self.root / "terminal.json").write_bytes(execution.encoded({"passed": True}))
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=checkpoint)

    def test_active_owner_rejects_self_consistent_forged_appended_bundle(self):
        authority = self.authority()
        fake = {"protocol": execution.PROTOCOL, "mode": "physical", "passed": True}
        (self.root / "terminal.json").write_bytes(execution.encoded(fake))
        (self.root / "verifier.json").write_bytes(execution.encoded({"terminal_sha256": execution.digest(fake)}))
        with self.assertRaises(execution.ExecutionError):
            authority.verified_execution()

    def test_checkpoint_detects_config_tamper_and_deletion(self):
        authority = self.authority()
        checkpoint = authority.checkpoint()
        authority.close()
        path = self.root / "config.json"
        original = path.read_bytes()
        value = json.loads(original)
        value["mode"] = "physical"
        path.write_bytes(execution.encoded(value))
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=checkpoint)
        path.write_bytes(original)
        path.unlink()
        with self.assertRaises(execution.ExecutionError):
            self.authority(checkpoint=checkpoint)

    def test_fixture_intent_never_authenticates_and_never_reexecutes(self):
        authority = self.authority()
        with self.assertRaises(execution.ExecutionError):
            authority.execute_once()
        self.assertTrue((self.root / "intent.json").is_file())
        with patch.object(authority, "_dispatch", side_effect=AssertionError("redispatched")):
            with self.assertRaises(execution.ExecutionUnknown):
                authority.execute_once()
        with self.assertRaises(execution.ExecutionError):
            authority.verified_execution()

    def test_six_trajectory_barrier_exact_source_and_irreversibility(self):
        reg = registration(self.store, self.policy, self.runtime, purpose="independent_acceptance")
        own = reg.gate.binding.subject
        subjects = tuple(replace(own, trajectory_id=name) for name in TRAJECTORIES)
        complete = CohortFreeze(subjects, "c" * 64, "d" * 64, True)
        for index, freeze in enumerate((replace(complete, subjects=subjects[:-1]),
                replace(complete, no_further_model_actions=False),
                replace(complete, subjects=(subjects[0], replace(subjects[1], milestone="M1"), *subjects[2:])),
                replace(complete, subjects=(subjects[0], replace(subjects[1], requirements_sha256="0" * 64), *subjects[2:])),
                replace(complete, subjects=(replace(own, source_sha256="0" * 64), *subjects[1:])))):
            authority = self.authority(root=self.root.with_name(self.root.name + str(index)), reg=reg, freeze=lambda: freeze)
            with self.assertRaises(execution.ExecutionError):
                authority.execute_once()
            self.assertFalse((authority.root / "intent.json").exists())
        authority = self.authority(reg=reg, freeze=lambda: complete)
        self.assertEqual(authority._freeze(), complete)
        with self.assertRaises(execution.ExecutionError):
            authority.execute_once()
        self.assertEqual(json.loads((self.root / "intent.json").read_bytes())["freeze"]["receipt_sha256"], "c" * 64)
        sequence = iter((complete, replace(complete, no_further_model_actions=False)))
        volatile = self.authority(root=self.root.with_name(self.root.name + "changed"), reg=reg, freeze=lambda: next(sequence))
        with patch.object(volatile, "_dispatch", return_value=None):
            with self.assertRaisesRegex(execution.ExecutionError, "barrier"):
                volatile.execute_once()
        self.assertTrue((volatile.root / "intent.json").is_file())
        self.assertFalse((volatile.root / "terminal.json").exists())
        with self.assertRaises(execution.ExecutionUnknown):
            volatile.execute_once()

    def test_loaded_source_change_and_unsafe_source_rejected_before_dispatch(self):
        authority = self.authority()
        with patch.object(execution, "evaluator_sources", return_value={"changed": "0" * 64}):
            with self.assertRaises(execution.ExecutionError):
                authority.execute_once()
        self.assertFalse((self.root / "intent.json").exists())
        commit = self.store.propose({".env.local": "not-a-secret-test-fixture"})
        with self.assertRaises(execution.ExecutionError):
            execution.capture_git_source(self.store, commit)

    def test_checkpoint_sink_failure_stops_before_dispatch(self):
        calls = []
        def sink(checkpoint):
            calls.append(checkpoint)
            if len(calls) == 2:
                raise RuntimeError("trusted checkpoint sink unavailable")
        authority = execution.CandidateReleaseExecution(self.root, self.store, self.reg, self.policy,
                                                         mode="fixture", checkpoint_sink=sink)
        self.authorities.append(authority)
        with patch.object(authority, "_dispatch", side_effect=AssertionError("dispatched after sink failure")):
            with self.assertRaisesRegex(RuntimeError, "checkpoint sink"):
                authority.execute_once()
        self.assertEqual(len(calls), 2)
        self.assertTrue((self.root / "intent.json").exists())
        with self.assertRaises(execution.ExecutionUnknown):
            authority.execute_once()

    def test_owned_volume_requires_exact_quota_labels_driver_and_actual_mount(self):
        authority = self.authority()
        intent = {"volume": "gossip-release-volume-" + "a" * 32, "execution_id": "release-" + "b" * 32}
        identity = {"Name": intent["volume"], "Driver": "local", "Scope": "local", "Options": execution.VOLUME_OPTIONS,
                    "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}}
        variants = [identity, identity | {"Driver": "other"}, identity | {"Name": "foreign"},
                    identity | {"Options": {"type": "tmpfs", "device": "tmpfs", "o": "size=32g"}},
                    identity | {"Labels": {}}, identity | {"Scope": "global"}]
        for index, value in enumerate(variants):
            label = "volume-test-" + str(index)
            raw = execution.encoded(value)
            authority._retain(label + "-stdout.bin", raw)
            authority._retain(label + "-stderr.bin", b"")
            record = {"exit_code": 0, "timed_out": False, "capture_complete": True}
            for kind, content in (("stdout", raw), ("stderr", b"")):
                record[kind] = {"path": label + "-" + kind + ".bin", "sha256": execution.raw_digest(content),
                    "bytes": len(content), "observed_bytes": len(content), "truncated": False}
            self.assertEqual(authority._volume_valid(record, intent), index == 0)
        args = authority._arguments("fixture-name", self.root / "source", self.root / "inputs", volume=intent["volume"])
        self.assertFalse(any(arg.startswith("--tmpfs=/tmp:") for arg in args))
        self.assertIn("type=volume,source=" + intent["volume"] + ",target=/tmp,volume-nocopy", args)
        self.assertIn("--read-only", args)
        self.assertIn("--cap-drop=ALL", args)
        self.assertIn("--network=none", args)

    def test_mocked_docker_exec_failure_is_infrastructure_and_not_retried(self):
        # Explicit host-command mocks: this tests normalization, not physical work.
        runtime = {"test_fixture": "mocked-runtime-never-study-evidence"}
        reg = registration(self.store, self.policy, runtime)
        with patch.object(execution, "runtime_identity", return_value=runtime):
            authority = execution.CandidateReleaseExecution(self.root, self.store, reg, self.policy)
            self.authorities.append(authority)
            count = []
            def command(label, arguments, *, limit, timeout):
                count.append(label)
                intent = json.loads((authority.root / "intent.json").read_bytes())
                volume = intent["volume"]
                identity = {"Name": volume, "Driver": "local", "Scope": "local", "Options": execution.VOLUME_OPTIONS,
                            "Labels": {"gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}}
                if label == "build-state":
                    stdout = execution.encoded({"State": {"Running": True, "Paused": True, "Pid": 123},
                        "Mounts": [{"Destination": "/tmp", "Type": "volume", "Name": volume, "Driver": "local", "RW": True}]})
                elif label in ("volume-created", "volume-cleanup-inspect"):
                    stdout = execution.encoded(identity)
                elif label == "volume-create":
                    stdout = volume.encode() + b"\n"
                else:
                    stdout = b""
                stderr = b"simulated Docker daemon exec failure" if label == "build" else b""
                record = {"arguments": arguments, "exit_code": 125 if label == "build" else 0,
                          "timed_out": False, "capture_complete": True}
                for kind, raw in (("stdout", stdout), ("stderr", stderr)):
                    filename = label + "-" + kind + ".bin"
                    authority._retain(filename, raw)
                    record[kind] = {"path": filename, "sha256": execution.raw_digest(raw), "bytes": len(raw),
                                    "observed_bytes": len(raw), "truncated": False}
                authority._retain(label + ".json", execution.encoded(record))
                return record
            with patch.object(authority, "_command", side_effect=command), patch.object(execution.DockerValidator, "_remove", return_value=True):
                result = authority.execute_once()
                self.assertEqual(result.terminal_status, "infrastructure_error")
                self.assertEqual([item.status for item in result.outcomes], ["infrastructure_error"] * 3)
                original = list(count)
                self.assertEqual(authority.execute_once(), result)
                self.assertEqual(count, original)


    def test_product_pin_and_milestone_are_enforced_before_any_journal_or_runtime(self):
        original = self.reg.gate.binding.subject
        for subject in (replace(original, requirements_sha256=legacy_observer.PRODUCT_CONTRACT_SHA256),
                        replace(original, milestone="M3")):
            with self.assertRaisesRegex(execution.ExecutionError, "M4 product-v2"):
                execution.binding_for(subject, self.policy, self.runtime, purpose="public_release")
            gate = replace(self.reg.gate, binding=replace(self.reg.gate.binding, subject=subject))
            with self.assertRaisesRegex(execution.ExecutionError, "exact M4 product"):
                replace(self.reg, gate=gate)
        for requirements in (("M4-RELEASE-HANDOFF", "M4-INTERFACE-COMPLETE"), ("M1-INTAKE",)):
            with self.assertRaisesRegex(execution.ExecutionError, "fixed partial release profile"):
                replace(self.reg, gate=replace(self.reg.gate, requirement_ids=requirements))
        self.assertFalse(self.root.exists())

    def test_v1_bindings_and_dataclass_capabilities_cannot_be_relabelled_as_v2(self):
        tree, files = execution.capture_git_source(self.store, self.store.head())
        old_subject = replace(self.reg.gate.binding.subject,
            requirements_sha256=legacy_observer.PRODUCT_CONTRACT_SHA256,
            source_sha256=legacy_execution.source_sha256(files))
        old_policy = legacy_execution.ReleasePolicy(legacy_observer.RUNTIME_IMAGE)
        old_binding = legacy_execution.binding_for(old_subject, old_policy, self.runtime, purpose="public_release")
        self.assertNotEqual(old_binding.execution_protocol, self.reg.gate.binding.execution_protocol)
        self.assertNotEqual(old_binding.evaluator_sha256, self.reg.gate.binding.evaluator_sha256)
        self.assertNotEqual(old_binding.ordered_suite_sha256, self.reg.gate.binding.ordered_suite_sha256)
        self.assertNotEqual(legacy_execution.source_sha256(files), execution.source_sha256(files))
        old_gate = replace(self.reg.gate, binding=old_binding)
        old_registration = legacy_execution.Registration(old_gate, self.store.head(), tree,
                                                         self.reg.repetition_id, TRAJECTORIES)
        with self.assertRaisesRegex(execution.ExecutionError, "Typed immutable"):
            execution.CandidateReleaseExecution(self.root, self.store, old_registration, self.policy, mode="fixture")
        self.assertFalse(self.root.exists())
        # Product pin alone cannot relabel a v1 suite/evaluator/protocol binding.
        relabelled = replace(old_binding, subject=self.reg.gate.binding.subject)
        with self.assertRaisesRegex(execution.ExecutionError, "execution protocol"):
            replace(self.reg, gate=replace(self.reg.gate, binding=relabelled))
        forged = replace(relabelled, execution_protocol=execution.PROTOCOL)
        with self.assertRaisesRegex(execution.ExecutionError, "fixed observer"):
            self.authority(reg=replace(self.reg, gate=replace(self.reg.gate, binding=forged)))

    def test_v1_external_checkpoint_cannot_authenticate_v2_journal(self):
        authority = self.authority()
        checkpoint = authority.checkpoint()
        authority.close()
        old_checkpoint = legacy_execution.ControllerCheckpoint(checkpoint.files)
        with self.assertRaisesRegex(execution.ExecutionError, "External checkpoint"):
            self.authority(checkpoint=old_checkpoint)

    def test_loaded_v2_authority_sources_exclude_v1_executor_and_observer(self):
        sources = execution.evaluator_sources()
        self.assertEqual(set(sources), {"candidate_release_execution_v2.py", "candidate_release_observer_v2.py",
            "project_acceptance_registry_v1.py", "sandbox.py", "gitstore.py"})
        self.assertEqual(sources['candidate_release_observer_v2.py'], observer.LOADED_SOURCE_SHA256)
        original = legacy_execution.evaluator_sources()
        self.authority()
        self.assertEqual(legacy_execution.evaluator_sources(), original)


class CandidateReleaseV2ExecutionCrashTests(unittest.TestCase):
    def test_real_child_exit_after_intent_preserves_unknown_and_no_retry(self):
        with ArtifactDirectory("candidate-release-v2-unknown-child", retain_success=True) as artifacts:
            script = r'''
from dataclasses import asdict
import json, os, sys
from pathlib import Path
from gossip_harness import candidate_release_execution_v2 as e
from gossip_harness import candidate_release_observer_v2 as o
from tests.test_candidate_release_execution_v2 import make_store, registration
root = Path(sys.argv[1]).resolve()
store = make_store(root/'repo.git', {'library/__init__.py': b'# no execution\n'})
policy = e.ReleasePolicy(o.RUNTIME_IMAGE)
reg = registration(store, policy, {'kind':'fixture-no-Docker'})
def sink(checkpoint):
    (root/'external-checkpoint.json').write_bytes(e.encoded(asdict(checkpoint)))
authority = e.CandidateReleaseExecution(root/'journal', store, reg, policy, mode='fixture', checkpoint_sink=sink)
def terminate(intent):
    os._exit(91)
authority._dispatch = terminate
authority.execute_once()
'''
            process = subprocess.run([sys.executable, "-c", script, str(artifacts.root)],
                                     capture_output=True, timeout=20, check=False)
            (artifacts.root / "child.stdout").write_bytes(process.stdout)
            (artifacts.root / "child.stderr").write_bytes(process.stderr)
            self.assertEqual(process.returncode, 91, process.stderr)
            checkpoint_raw = json.loads((artifacts.root / "external-checkpoint.json").read_bytes())
            checkpoint = execution.ControllerCheckpoint(tuple(tuple(item) for item in checkpoint_raw["files"]))
            store = GitStore(artifacts.root.resolve() / "repo.git")
            policy = execution.ReleasePolicy(observer.RUNTIME_IMAGE)
            reg = registration(store, policy, {"kind": "fixture-no-Docker"})
            with execution.CandidateReleaseExecution(artifacts.root.resolve() / "journal", store, reg, policy,
                    mode="fixture", expected_checkpoint=checkpoint) as authority:
                with patch.object(authority, "_dispatch", side_effect=AssertionError("reran unknown attempt")):
                    with self.assertRaises(execution.ExecutionUnknown):
                        authority.execute_once()
