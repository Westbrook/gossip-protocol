"""Actual bundle-byte/quarantine checks; no candidate execution or Docker."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.gitstore import GitStore, _run
from gossip_harness.peer_git_bundle_v1 import (
    BundleError, MAX_BUNDLE_BYTES, PROFILE, PROTOCOL, _Git, _header, _manifest, export_bundle, import_bundle,
)


def descriptor():
    return {"protocol": PROTOCOL, "profile": PROFILE, "object_format": "sha1",
            "base_sha": "1" * 40, "offered_sha": "2" * 40, "offered_ref": "refs/harness/proposals/" + "2" * 40,
            "bundle_sha256": "0" * 64, "bundle_bytes": 300, "object_count": 3, "commit_count": 1,
            "expanded_bytes": 100, "objects_sha256": "0" * 64, "tree_sha": "3" * 40,
            "files_sha256": "0" * 64, "file_count": 1, "source_bytes": 10}


class BundleMetadataTests(unittest.TestCase):
    def test_descriptor_strict_types_and_resource_bounds(self):
        self.assertEqual(_manifest(descriptor()), descriptor())
        for key, value in (("bundle_bytes", True), ("bundle_bytes", MAX_BUNDLE_BYTES + 1),
                           ("object_count", 4097), ("offered_ref", "refs/heads/accepted"),
                           ("tree_sha", "tree-ish"), ("object_format", []), ("object_format", {})):
            with self.subTest(key=key, value=value):
                changed = descriptor()
                changed[key] = value
                with self.assertRaises(BundleError):
                    _manifest(changed)

    def test_header_rejects_prerequisites_extra_refs_and_partial_bundles(self):
        manifest = descriptor()
        prefix = b"# v3 git bundle\n@object-format=sha1\n"
        ref = (manifest["offered_sha"] + " " + manifest["offered_ref"]).encode()
        _header(prefix + ref + b"\n\nPACKpayload", manifest)
        for extra in (b"-" + b"1" * 40 + b" required\n", b"@filter=blob:none\n",
                      b"2" * 40 + b" refs/heads/accepted\n"):
            with self.subTest(extra=extra):
                with self.assertRaises(BundleError):
                    _header(prefix + extra + ref + b"\n\nPACKpayload", manifest)

    def test_payload_accepts_only_bounded_immutable_bytes_before_allocating_root(self):
        with ArtifactDirectory("peer-bundle-metadata", retain_success=True) as artifacts:
            target = artifacts.root / "not-created"
            for payload in (b"", bytearray(b"test"), "sender/path.bundle", b"x" * (MAX_BUNDLE_BYTES + 1)):
                with self.subTest(kind=type(payload).__name__):
                    with self.assertRaises(BundleError):
                        import_bundle(payload, descriptor(), target)
                    self.assertFalse(target.exists())


class GitBundleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("peer-git-bundles", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.root = cls.artifacts.root
        cls.base = GitStore.create(cls.root / "baseline.git", {
            "alpha.json": '{"value":1}\n', "beta.json": '{"value":10}\n'})
        cls.base_sha = cls.base.head()
        cls.sender = GitStore.fork(cls.base, cls.root / "sender.git")
        cls.offered = cls.sender.propose({"alpha.json": '{"value":8}\n'})
        cls.payload, cls.manifest = export_bundle(cls.sender, cls.offered, cls.base_sha)

    def root_for(self, suffix):
        return self.root / (self._testMethodName + "-" + suffix)

    def receive(self, suffix="receive"):
        return import_bundle(self.payload, self.manifest, self.root_for(suffix))

    def target(self):
        return GitStore.fork(self.base, self.root_for("target.git"))

    def pin_checkout(self, source, operation):
        with source._checkout(self.base_sha) as checkout:
            operation(checkout)
            _run(checkout, "commit", "--allow-empty", "-m", "Unsupported profile fixture")
            offered = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
            source._git("fetch", "--no-tags", str(checkout), f"{offered}:refs/harness/proposals/{offered}")
        return offered

    def test_receiver_uses_transferred_bytes_with_sender_unavailable(self):
        self.sender.path.rename(self.root / "sender-retained-offline.git")
        quarantine = self.receive()
        self.assertEqual(quarantine.read_files(self.offered)["alpha.json"], '{"value":8}\n')
        self.assertFalse((quarantine.path / "objects/info/alternates").exists())
        self.assertEqual(quarantine._git("for-each-ref", "--format=%(objectname) %(refname)"),
                         self.offered + " " + self.manifest["offered_ref"])
        self.assertEqual(self.base.head(), self.base_sha)
        receipt = json.loads((self.root_for("receive") / "receipt.json").read_text())
        self.assertEqual(receipt["status"], "quarantined")
        self.assertFalse(receipt["publication_authorized"])
        self.assertFalse(receipt["candidate_execution"])
        self.assertEqual(receipt["inspection"]["files"][0]["sha256"], hashlib.sha256(b'{"value":8}\n').hexdigest())

    def test_corrupt_bytes_and_corrupt_pack_preserve_failure_artifacts(self):
        corrupted = self.payload[:-1] + bytes([self.payload[-1] ^ 1])
        first = self.root_for("digest-failure")
        with self.assertRaises(BundleError):
            import_bundle(corrupted, self.manifest, first)
        self.assertEqual((first / "received.bundle").read_bytes(), corrupted)
        self.assertTrue((first / "failure.json").exists())
        changed = deepcopy(self.manifest)
        changed["bundle_sha256"] = hashlib.sha256(corrupted).hexdigest()
        second = self.root_for("pack-failure")
        with self.assertRaises(BundleError):
            import_bundle(corrupted, changed, second)
        self.assertFalse((second / "quarantine.git/gossip-harness-store").exists())
        self.assertTrue((second / "failure.json").exists())

    def test_receiver_census_rejects_descriptor_substitution(self):
        altered = deepcopy(self.manifest)
        altered["files_sha256"] = "f" * 64
        root = self.root_for("mismatch")
        with self.assertRaises(BundleError):
            import_bundle(self.payload, altered, root)
        self.assertFalse((root / "quarantine.git/gossip-harness-store").exists())
        with self.assertRaises(FileExistsError):
            import_bundle(self.payload, self.manifest, root)

    def test_divergent_merged_tree_is_validated_and_cas_remains_separate(self):
        quarantine, target = self.receive(), self.target()
        other = target.propose({"beta.json": '{"value":9}\n'})
        prepared = target.prepare(target, other, self.base_sha, lambda _: (True, "known fixture"), ("beta.json",))
        target.accept(prepared)
        before, inspected = target.head(), []

        def check(checkout):
            values = tuple(json.loads((checkout / name).read_text())["value"] for name in ("alpha.json", "beta.json"))
            inspected.append(values)
            return values[0] <= values[1], "Trusted JSON invariant fixture"

        candidate = target.prepare(quarantine, self.offered, before, check, ("alpha.json",))
        self.assertEqual(candidate.status, "prepared", candidate.detail)
        self.assertNotEqual(candidate.candidate_sha, self.offered)
        self.assertEqual(inspected, [(8, 9)])
        self.assertEqual(target.head(), before)
        accepted = target.accept(candidate)
        self.assertEqual(accepted.new_head, candidate.candidate_sha)
        noop = target.prepare(quarantine, self.offered, target.head(), check, ("alpha.json",))
        self.assertEqual(noop.status, "noop")
        self.assertEqual(len(inspected), 1, "Noop is not fresh validation")

    def test_real_clean_merge_can_fail_and_stale_head_does_not_gain_a_pass(self):
        quarantine, target = self.receive(), self.target()
        other = target.propose({"beta.json": '{"value":5}\n'})
        first = target.prepare(target, other, self.base_sha, lambda _: (True, "known fixture"), ("beta.json",))
        target.accept(first)
        called = []

        def invariant(checkout):
            called.append(True)
            alpha = json.loads((checkout / "alpha.json").read_text())["value"]
            beta = json.loads((checkout / "beta.json").read_text())["value"]
            return alpha <= beta, "Alpha must not exceed beta"

        before = target.head()
        failed = target.prepare(quarantine, self.offered, before, invariant, ("alpha.json",))
        self.assertEqual(failed.status, "validation_failed")
        self.assertEqual(target.head(), before)
        stale = target.prepare(quarantine, self.offered, self.base_sha, invariant, ("alpha.json",))
        self.assertEqual(stale.status, "stale")
        self.assertEqual(called, [True])

    def test_reverted_scope_escape_is_preserved_through_bundle_import(self):
        source, target = GitStore.fork(self.base, self.root_for("source.git")), self.target()
        first = source.propose({"beta.json": '{"value":999}\n'})
        offered = source.propose({"beta.json": '{"value":10}\n', "alpha.json": '{"value":2}\n'}, base_sha=first)
        payload, manifest = export_bundle(source, offered, self.base_sha)
        quarantine = import_bundle(payload, manifest, self.root_for("scope-receive"))
        called = []
        candidate = target.prepare(quarantine, offered, self.base_sha,
                                   lambda _: (called.append(True) or True, "fixture"), ("alpha.json",))
        self.assertEqual(candidate.status, "scope_rejected")
        self.assertIn("beta.json", candidate.changed_paths)
        self.assertEqual(called, [])
        self.assertEqual(target.head(), self.base_sha)

    def test_unsupported_historical_dot_path_is_not_hidden_by_revert(self):
        source = GitStore.fork(self.base, self.root_for("source.git"))
        first = source.propose({".attributes": "unsupported profile fixture\n"})
        offered = source.propose({".attributes": None}, base_sha=first)
        with self.assertRaises(BundleError):
            export_bundle(source, offered, self.base_sha)

    def test_symlink_and_case_colliding_directory_trees_are_rejected(self):
        symlinks = GitStore.fork(self.base, self.root_for("symlink.git"))

        def symlink(checkout):
            os.symlink("alpha.json", checkout / "link")
            _run(checkout, "add", "link")

        offered = self.pin_checkout(symlinks, symlink)
        with self.assertRaises(BundleError):
            export_bundle(symlinks, offered, self.base_sha)
        collisions = GitStore.fork(self.base, self.root_for("collision.git"))
        blob = collisions._git("rev-parse", self.base_sha + ":alpha.json")

        def collide(checkout):
            for name in ("Dir/a.txt", "dir/b.txt"):
                _run(checkout, "update-index", "--add", "--cacheinfo", f"100644,{blob},{name}")

        offered = self.pin_checkout(collisions, collide)
        with self.assertRaises(BundleError):
            export_bundle(collisions, offered, self.base_sha)

    def test_unsafe_empty_directory_tree_is_not_omitted_from_profile_checks(self):
        source = GitStore.fork(self.base, self.root_for("source.git"))
        log = self.root_for("fixture-git")
        log.mkdir()
        git = _Git(log)
        empty = git.call(source.path, "hash-object", "-w", "-t", "tree", "--stdin", input_bytes=b"").decode().strip()
        entries = git.call(source.path, "ls-tree", self.base_sha)
        entries += f"040000 tree {empty}\t.hidden\n".encode()
        tree = git.call(source.path, "mktree", input_bytes=entries).decode().strip()
        offered = git.call(source.path, "-c", "user.name=Bundle fixture", "-c", "user.email=fixture@example.invalid",
                           "commit-tree", tree, "-p", self.base_sha, "-m", "Empty directory fixture").decode().strip()
        source._git("update-ref", "refs/harness/proposals/" + offered, offered)
        with self.assertRaises(BundleError):
            export_bundle(source, offered, self.base_sha)


class BundleProcessBoundaryTests(unittest.TestCase):
    def test_exited_leader_does_not_leave_pipe_holding_descendant(self):
        with ArtifactDirectory("bundle-process-cleanup", retain_success=True) as artifacts:
            executable = artifacts.root / "fixed-git-substitute"
            executable.write_text("#!" + sys.executable + "\n"
                "import os,time\n"
                "child=os.fork()\n"
                "if child==0:\n"
                "    time.sleep(60)\n"
                "    os._exit(0)\n"
                "os.write(1,(str(child)+'\\n').encode())\n"
                "os._exit(0)\n")
            executable.chmod(0o700)
            git = _Git(artifacts.root)
            git.executable = str(executable)
            started = time.monotonic()
            with self.assertRaisesRegex(BundleError, "readers did not finish"):
                git.call(None, "fixed-fixture")
            receipt = json.loads((artifacts.root / "git-0000.json").read_text())
            self.assertTrue(receipt["readers_finished"])
            self.assertGreater(int((artifacts.root / "git-0000.stdout").read_text()), 0)
            # The fixed child keeps its pipes for60s unless killed. EOF before
            # then proves quiescence without mistaking an OS-reaping zombie for
            # a surviving executing process on a minimal Linux container host.
            self.assertLess(time.monotonic() - started, 30)


if __name__ == "__main__":
    unittest.main()
