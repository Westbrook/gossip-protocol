"""Fresh physical candidate package qualification; no comparative study claims."""
from __future__ import annotations

from dataclasses import asdict
import json
import os
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_release_execution_v1 as execution
from gossip_harness import candidate_release_observer_v1 as observer
from gossip_harness.library_m4_reference_v1 import m4_binary_files, m4_files
from gossip_harness.sandbox import DockerValidator
from tests.test_candidate_release_execution_v1 import make_store, registration


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateReleaseDockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready, detail = DockerValidator(observer.RUNTIME_IMAGE, {"unused.py": ""}).preflight()
        if not ready:
            raise RuntimeError(detail)

    def qualify(self, label, *, defect=False):
        files = {name: value.encode("utf-8") for name, value in m4_files().items()} | m4_binary_files()
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
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            store = make_store(artifacts.root / "candidate.git", files)
            policy = execution.ReleasePolicy(observer.RUNTIME_IMAGE)
            runtime = execution.runtime_identity(policy)
            reg = registration(store, policy, runtime)
            with execution.CandidateReleaseExecution(artifacts.root.resolve() / "execution", store, reg, policy) as authority:
                result = authority.execute_once()
                checkpoint = authority.checkpoint()
                (artifacts.root / "external-checkpoint.json").write_bytes(execution.encoded(asdict(checkpoint)))
                (artifacts.root / "normalized-execution.json").write_bytes(execution.encoded(asdict(result)))
                self.assertEqual(authority.verified_execution(), result)
            with execution.CandidateReleaseExecution(artifacts.root.resolve() / "execution", store, reg, policy,
                    expected_checkpoint=checkpoint) as reopened:
                self.assertEqual(reopened.execute_once(), result)
                self.assertEqual(reopened.checkpoint(), checkpoint)
            terminal = json.loads((artifacts.root / "execution/terminal.json").read_bytes())
            self.assertTrue(all(terminal["cleanup"].values()))
            self.assertEqual(len(terminal["cleanup"]), 2)
            self.assertTrue(terminal["volume_cleanup"])
            self.assertEqual(result.terminal_status, "completed", terminal)
            return result, terminal

    def test_actual_frozen_reference_package_is_executed_from_captured_capsule(self):
        result, terminal = self.qualify("candidate-release-valid")
        self.assertEqual([row.status for row in result.outcomes], ["passed"] * 3, terminal)
        self.assertEqual(terminal["commands"], ["volume-before", "volume-create", "volume-created", "build-start", "build", "build-pause", "build-state", "capsule",
            "consumer-start", "cli-import", "cli-list", "cli-show", "cli-export",
            "volume-cleanup-inspect", "volume-remove", "volume-after"])

    def test_modified_emitted_package_fails_host_manifest_and_cli_observations(self):
        result, terminal = self.qualify("candidate-release-mutant", defect=True)
        self.assertEqual([row.status for row in result.outcomes], ["passed", "failed", "failed"], terminal)
