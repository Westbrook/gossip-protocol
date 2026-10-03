"""Fresh physical candidate package qualification; no comparative study claims."""
from __future__ import annotations

from dataclasses import asdict
import json
import os
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_release_execution_v2 as execution
from gossip_harness import candidate_release_observer_v2 as observer
from gossip_harness.candidate_scope_consumer_v1 import V2ReleaseObservationSource
from gossip_harness.library_v2_reference_v1 import v2_binary_files, v2_files
from gossip_harness.sandbox import DockerValidator
from tests.test_candidate_release_execution_v2 import make_store, registration


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateReleaseV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready, detail = DockerValidator(observer.RUNTIME_IMAGE, {"unused.py": ""}).preflight()
        if not ready:
            raise RuntimeError(detail)

    def qualify(self, label, *, defect=False, alternate_manifest=False):
        files = {name: value.encode("utf-8") for name, value in v2_files().items()} | v2_binary_files()
        if defect:
            # The builder still emits the exact bound source and manifest; only
            # the delivered CLI is altered after publication, outside source.
            path = "library/clients/release.py"
            original = files[path].decode("utf-8")
            seam = "if __name__ == '__main__':"
            self.assertEqual(original.count(seam), 1)
            replacement = '''if __name__ == "__main__":
    import atexit
    def _qualification_corrupt_delivered_package():
        from pathlib import Path
        target = Path('/tmp/release/library/__main__.py')
        if target.is_file():
            target.write_text('print("null")\\n', encoding='utf-8')
    atexit.register(_qualification_corrupt_delivered_package)
'''
            files[path] = original.replace(seam, replacement).encode("utf-8")
        if alternate_manifest:
            self.assertFalse(defect)
            path = "library/clients/release.py"
            original = files[path].decode("utf-8")
            seam = "if __name__ == '__main__':"
            self.assertEqual(original.count(seam), 1)
            replacement = '''if __name__ == "__main__":
    import atexit
    def _qualification_reserialize_manifest():
        import json
        from pathlib import Path
        target = Path('/tmp/release/release-manifest.json')
        if target.is_file():
            value = json.loads(target.read_text(encoding='utf-8'))
            reordered = {name: value[name] for name in reversed(list(value))}
            target.write_text(json.dumps(reordered, ensure_ascii=False, indent=2) + "\\n", encoding='utf-8')
    atexit.register(_qualification_reserialize_manifest)
'''
            files[path] = original.replace(seam, replacement).encode("utf-8")
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            store = make_store(artifacts.root / "candidate.git", files)
            policy = execution.ReleasePolicy(observer.RUNTIME_IMAGE)
            runtime = execution.runtime_identity(policy)
            reg = registration(store, policy, runtime)
            checkpoints = []
            external = artifacts.root / "external-checkpoints"
            external.mkdir()
            def retain_checkpoint(checkpoint):
                # Independent controller-side records, outside the journal.
                raw = execution.encoded(asdict(checkpoint))
                with (external / f"{len(checkpoints):04d}.json").open("xb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                checkpoints.append(checkpoint)
            with execution.CandidateReleaseExecution(artifacts.root.resolve() / "execution", store, reg, policy,
                    checkpoint_sink=retain_checkpoint) as authority:
                result = authority.execute_once()
                checkpoint = authority.checkpoint()
                (artifacts.root / "external-checkpoint.json").write_bytes(execution.encoded(asdict(checkpoint)))
                (artifacts.root / "normalized-execution.json").write_bytes(execution.encoded(asdict(result)))
                self.assertEqual(authority.verified_execution(), result)
                external_raw = json.loads((artifacts.root / "external-checkpoint.json").read_bytes())
                retained_checkpoint = execution.ControllerCheckpoint(tuple(tuple(item) for item in external_raw["files"]))
                bridge = V2ReleaseObservationSource(authority, retained_checkpoint)
                bridged = bridge.observation(reg.gate, None)
                self.assertEqual(bridged.execution, result)
                self.assertEqual(bridged.binding, result.binding)
                self.assertEqual(bridged.gate_id, reg.gate.gate_id)
                self.assertEqual(bridged.mode, "physical")
                self.assertIsNone(bridged.reuse_receipt_sha256)
                self.assertEqual(authority.checkpoint(), retained_checkpoint)
                (artifacts.root / "bridge-observation.json").write_bytes(execution.encoded(asdict(bridged)))
                self.assertEqual(checkpoints[-1], checkpoint)
                self.assertGreater(len(checkpoints), 40)
                for before, after in zip(checkpoints, checkpoints[1:]):
                    self.assertEqual(len(after.files), len(before.files) + 1)
                    self.assertTrue(set(before.files).issubset(after.files))
            with execution.CandidateReleaseExecution(artifacts.root.resolve() / "execution", store, reg, policy,
                    expected_checkpoint=checkpoint) as reopened:
                self.assertEqual(reopened.execute_once(), result)
                self.assertEqual(reopened.checkpoint(), checkpoint)
                reopened_bridge = V2ReleaseObservationSource(reopened, retained_checkpoint)
                self.assertEqual(reopened_bridge.observation(reg.gate, None), bridged)
                self.assertEqual(reopened.checkpoint(), retained_checkpoint)
            self.assertEqual(len(tuple(external.glob("*.json"))), len(checkpoints))
            for index, checkpoint_item in enumerate(checkpoints):
                self.assertEqual((external / f"{index:04d}.json").read_bytes(),
                                 execution.encoded(asdict(checkpoint_item)))
            terminal = json.loads((artifacts.root / "execution/terminal.json").read_bytes())
            self.assertTrue(all(terminal["cleanup"].values()))
            self.assertEqual(len(terminal["cleanup"]), 2)
            self.assertTrue(terminal["volume_cleanup"])
            self.assertEqual(result.terminal_status, "completed", terminal)
            return result, terminal

    def test_actual_frozen_reference_package_is_executed_from_captured_capsule(self):
        result, terminal = self.qualify("candidate-release-v2-valid")
        self.assertEqual([row.status for row in result.outcomes], ["passed"] * 3, terminal)
        self.assertEqual(terminal["commands"], ["volume-before", "volume-create", "volume-created", "build-start", "build", "build-pause", "build-state", "capsule",
            "consumer-start", "cli-import", "cli-list", "cli-show", "cli-export",
            "volume-cleanup-inspect", "volume-remove", "volume-after"])

    def test_modified_emitted_package_fails_host_manifest_and_cli_observations(self):
        result, terminal = self.qualify("candidate-release-v2-mutant", defect=True)
        self.assertEqual([row.status for row in result.outcomes], ["passed", "failed", "failed"], terminal)

    def test_legal_alternate_manifest_encoding_preserves_all_three_delivery_passes(self):
        result, terminal = self.qualify("candidate-release-v2-alternate-manifest", alternate_manifest=True)
        self.assertEqual([row.status for row in result.outcomes], ["passed"] * 3, terminal)
