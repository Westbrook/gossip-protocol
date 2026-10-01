"""Real-Git tests for distributed repository ownership and acceptance."""

from dataclasses import replace
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from gossip_harness.gitstore import GitStore, _run


def valid(_checkout: Path) -> tuple[bool, str]:
    return True, "Fixture accepted"


class GitStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="test-gitstore-")
        self.root = Path(self.directory.name)
        self.store = GitStore.create(self.root / "main.git", {
            "alpha.json": '{"value": 1}\n',
            "beta.json": '{"value": 10}\n',
            "notes.txt": "original\n",
        })
        self.initial = self.store.head()

    def tearDown(self):
        self.directory.cleanup()

    def peer(self, name="peer"):
        return GitStore.fork(self.store, self.root / f"{name}.git")

    def accept_offer(self, source, sha, allowed_paths=None, validator=valid):
        prepared = self.store.prepare(source, sha, self.store.head(), validator, allowed_paths)
        self.assertEqual(prepared.status, "prepared", prepared.detail)
        result = self.store.accept(prepared)
        self.assertEqual(result.status, "accepted", result.detail)
        self.assertEqual(result.new_head, prepared.candidate_sha)
        return result

    def test_forks_have_independent_objects_and_refs(self):
        peer = self.peer()
        self.assertEqual(peer.head(), self.initial)
        self.assertFalse((peer.path / "objects/info/alternates").exists())
        source_inodes = {(path.stat().st_dev, path.stat().st_ino)
                         for path in (self.store.path / "objects").rglob("*") if path.is_file()}
        peer_inodes = {(path.stat().st_dev, path.stat().st_ino)
                       for path in (peer.path / "objects").rglob("*") if path.is_file()}
        self.assertTrue(source_inodes)
        self.assertTrue(peer_inodes)
        self.assertFalse(source_inodes & peer_inodes)
        proposal = peer.propose({"notes.txt": "peer change\n"})
        self.assertEqual(peer.head(), self.initial)
        self.assertEqual(self.store.head(), self.initial)
        self.assertNotEqual(proposal, self.initial)

    def test_prepare_preserves_head_and_accepts_the_exact_tested_tree(self):
        peer = self.peer()
        proposal = peer.propose({"notes.txt": "new\n"})
        inspected = []

        def inspect(checkout):
            inspected.append((checkout / "notes.txt").read_text())
            return True, "Inspected candidate"

        candidate = self.store.prepare(peer, proposal, self.initial, inspect)
        self.assertEqual(candidate.status, "prepared")
        self.assertEqual(candidate.candidate_sha, proposal)
        self.assertEqual(inspected, ["new\n"])
        self.assertEqual(self.store.head(), self.initial)
        self.assertEqual(self.store.read_files(candidate.candidate_sha)["notes.txt"], "new\n")
        result = self.store.accept(candidate)
        self.assertEqual(result.new_head, candidate.candidate_sha)
        self.assertEqual(self.store.head(), candidate.candidate_sha)
        self.assertTrue(self.store.is_ancestor(self.initial, result.new_head))
        self.assertTrue(self.store.is_ancestor(proposal, result.new_head))

    def test_duplicate_offer_is_noop_without_revalidation(self):
        peer = self.peer()
        proposal = peer.propose({"notes.txt": "new\n"})
        self.accept_offer(peer, proposal)
        before = self.store.head()

        def should_not_run(_checkout):
            self.fail("Already accepted offers should not be revalidated")

        duplicate = self.store.prepare(peer, proposal, before, should_not_run)
        self.assertEqual(duplicate.status, "noop")
        result = self.store.accept(duplicate)
        self.assertEqual(result.status, "noop")
        self.assertEqual(self.store.head(), before)

    def test_text_conflict_preserves_accepted_history(self):
        left, right = self.peer("left"), self.peer("right")
        first = left.propose({"notes.txt": "left edit\n"})
        second = right.propose({"notes.txt": "right edit\n"})
        self.accept_offer(left, first)
        before = self.store.head()
        rejected = self.store.prepare(right, second, before, valid)
        self.assertEqual(rejected.status, "text_conflict")
        self.assertIn("notes.txt", rejected.detail)
        self.assertEqual(self.store.head(), before)
        self.assertEqual(self.store.accept(rejected).status, "text_conflict")
        self.assertEqual(self.store.head(), before)

    def test_clean_merge_can_fail_semantic_validation(self):
        def ordered(checkout):
            alpha = json.loads((checkout / "alpha.json").read_text())["value"]
            beta = json.loads((checkout / "beta.json").read_text())["value"]
            return alpha <= beta, "alpha must not exceed beta"

        left, right = self.peer("left"), self.peer("right")
        first = left.propose({"alpha.json": '{"value": 8}\n'})
        second = right.propose({"beta.json": '{"value": 5}\n'})
        # Each proposal is valid on the original baseline.
        self.assertEqual(self.store.prepare(right, second, self.initial, ordered).status, "prepared")
        self.accept_offer(left, first, validator=ordered)
        before = self.store.head()
        rejected = self.store.prepare(right, second, before, ordered)
        self.assertEqual(rejected.status, "validation_failed")
        self.assertEqual(self.store.head(), before)
        self.assertEqual(self.store.read_files()["beta.json"], '{"value": 10}\n')

    def test_compare_and_swap_rejects_stale_tested_candidate(self):
        left, right = self.peer("left"), self.peer("right")
        first = left.propose({"alpha.json": '{"value": 2}\n'})
        second = right.propose({"beta.json": '{"value": 9}\n'})
        first_candidate = self.store.prepare(left, first, self.initial, valid)
        second_candidate = self.store.prepare(right, second, self.initial, valid)
        first_result = self.store.accept(first_candidate)
        stale = self.store.accept(second_candidate)
        self.assertEqual(stale.status, "stale")
        self.assertEqual(stale.new_head, first_result.new_head)
        self.assertEqual(self.store.head(), first_result.new_head)
        self.assertTrue(self.store.is_ancestor(first, self.store.head()))
        self.assertFalse(self.store.is_ancestor(second, self.store.head()))
        retried = self.store.prepare(right, second, self.store.head(), valid)
        self.assertEqual(self.store.accept(retried).status, "accepted")
        self.assertTrue(self.store.is_ancestor(first, self.store.head()))
        self.assertTrue(self.store.is_ancestor(second, self.store.head()))

    def test_stale_expected_head_rejects_before_validation(self):
        peer = self.peer()
        first = peer.propose({"notes.txt": "new\n"})
        self.accept_offer(peer, first)
        candidate = self.store.prepare(peer, first, self.initial, valid)
        self.assertEqual(candidate.status, "stale")

    def test_unpublished_inherited_dependency_exceeds_scope(self):
        peer = self.peer()
        alpha = peer.propose({"alpha.json": '{"value": 2}\n'})
        beta = peer.propose({"beta.json": '{"value": 9}\n'}, base_sha=alpha)
        rejected = self.store.prepare(peer, beta, self.initial, valid, ("beta.json",))
        self.assertEqual(rejected.status, "scope_rejected")
        self.assertEqual(rejected.changed_paths, ("alpha.json", "beta.json"))
        self.assertEqual(self.store.head(), self.initial)
        self.accept_offer(peer, alpha, ("alpha.json",))
        accepted = self.store.prepare(peer, beta, self.store.head(), valid, ("beta.json",))
        self.assertEqual(accepted.status, "prepared", accepted.detail)
        self.assertEqual(accepted.changed_paths, ("beta.json",))
        self.assertEqual(self.store.accept(accepted).status, "accepted")

    def test_scope_checks_hidden_reverted_history_not_only_net_diff(self):
        peer = self.peer()
        hidden = peer.propose({"alpha.json": '{"value": 99}\n'})
        restored = peer.propose({"alpha.json": '{"value": 1}\n',
                                 "beta.json": '{"value": 9}\n'}, base_sha=hidden)
        rejected = self.store.prepare(peer, restored, self.initial, valid, ("beta.json",))
        self.assertEqual(rejected.status, "scope_rejected")
        self.assertIn("alpha.json", rejected.changed_paths)
        self.assertEqual(peer.read_files(restored)["alpha.json"], self.store.read_files()["alpha.json"])

    def test_scope_treats_merge_resolution_filenames_as_literal_paths(self):
        peer = self.peer()
        alpha = peer.propose({"alpha.json": '{"value": 2}\n'})
        beta = peer.propose({"beta.json": '{"value": 9}\n'})
        with peer._checkout(alpha) as checkout:
            _run(checkout, "fetch", str(peer.path), beta)
            _run(checkout, "merge", "--no-ff", "--no-commit", beta)
            (checkout / ":(literal)hidden").write_text("outside owned paths\n")
            _run(checkout, "add", "--all")
            _run(checkout, "commit", "-m", "Merge with out-of-scope resolution")
            merged = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
            peer._git("fetch", str(checkout), f"{merged}:refs/harness/proposals/{merged}")
        candidate = self.store.prepare(peer, merged, self.initial, valid,
                                       ("alpha.json", "beta.json"))
        self.assertEqual(candidate.status, "scope_rejected")
        self.assertIn(":(literal)hidden", candidate.changed_paths)
        self.assertEqual(self.store.head(), self.initial)

    def test_file_deletion_preserves_commit_history(self):
        peer = self.peer()
        deletion = peer.propose({"notes.txt": None})
        result = self.accept_offer(peer, deletion)
        self.assertNotIn("notes.txt", self.store.read_files())
        self.assertEqual(self.store.read_files(self.initial)["notes.txt"], "original\n")
        self.assertTrue(self.store.is_ancestor(self.initial, result.new_head))

    def test_validator_mutation_and_exception_do_not_approve_tree(self):
        peer = self.peer()
        proposal = peer.propose({"notes.txt": "new\n"})

        def mutating(checkout):
            (checkout / "notes.txt").write_text("untested change\n")
            return True, "Passed"

        candidate = self.store.prepare(peer, proposal, self.initial, mutating)
        self.assertEqual(candidate.status, "validation_failed")
        self.assertIn("modified", candidate.detail)

        def failing(_checkout):
            raise RuntimeError("test runner failed")

        candidate = self.store.prepare(peer, proposal, self.initial, failing)
        self.assertEqual(candidate.status, "validation_failed")
        self.assertIn("test runner failed", candidate.detail)
        self.assertEqual(self.store.head(), self.initial)

    def test_unprepared_commit_cannot_be_accepted(self):
        peer = self.peer()
        proposal = peer.propose({"notes.txt": "new\n"})
        candidate = self.store.prepare(peer, proposal, self.initial, valid)
        with self.assertRaisesRegex(ValueError, "not prepared"):
            self.store.accept(replace(candidate, candidate_sha=self.initial))
        self.assertEqual(self.store.head(), self.initial)

    def test_validator_cannot_hide_mutation_using_git_index_flags(self):
        peer = self.peer()
        proposal = peer.propose({"notes.txt": "new\n"})

        def hidden_mutation(checkout):
            subprocess.run(["git", "-C", str(checkout), "update-index",
                            "--assume-unchanged", "notes.txt"], check=True, capture_output=True)
            (checkout / "notes.txt").write_text("different tested tree\n")
            return True, "Passed"

        candidate = self.store.prepare(peer, proposal, self.initial, hidden_mutation)
        self.assertEqual(candidate.status, "validation_failed")
        self.assertEqual(self.store.head(), self.initial)

    def test_equivalent_operations_have_deterministic_hashes(self):
        other = GitStore.create(self.root / "other.git", self.store.read_files())
        self.assertEqual(other.head(), self.initial)
        first = self.store.propose({"notes.txt": "new\n"})
        second = other.propose({"notes.txt": "new\n"})
        self.assertEqual(first, second)
        first_candidate = self.store.prepare(self.store, first, self.initial, valid)
        second_candidate = other.prepare(other, second, self.initial, valid)
        self.assertEqual(first_candidate.candidate_sha, second_candidate.candidate_sha)

    def test_invalid_paths_and_existing_destinations_are_rejected(self):
        for name in ("../escape", "/absolute", ".git/config", "a/../../escape", "a\\b"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.store.propose({name: "bad"})
        with self.assertRaises(FileExistsError):
            GitStore.create(self.store.path, {})
        with self.assertRaises(ValueError):
            GitStore(self.root)


if __name__ == "__main__":
    unittest.main()
