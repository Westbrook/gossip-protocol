from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Ledger
from gossip_harness.sustained_checkpoint import load_checkpoint, save_checkpoint


class CheckpointWriteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "nested" / "state.json"

    def test_hash_matches_exact_bytes_and_fsyncs_file_and_directory(self):
        state = {"unicode": "café", "nested": {"complete": True}}
        with patch("gossip_harness.sustained_checkpoint.os.fsync", wraps=os.fsync) as sync:
            sha = save_checkpoint(self.path, state)
        self.assertEqual(sha, hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.assertEqual(json.loads(self.path.read_bytes()), state)
        self.assertEqual(sync.call_count, 2)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_failure_before_replace_preserves_existing_checkpoint(self):
        original = save_checkpoint(self.path, {"version": 1})
        with patch("gossip_harness.sustained_checkpoint.os.replace", side_effect=OSError("Interrupted")):
            with self.assertRaises(OSError):
                save_checkpoint(self.path, {"version": 2})
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), original)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_invalid_json_state_cannot_replace_previous_checkpoint(self):
        original = save_checkpoint(self.path, {"version": 1})
        for state in ([1], {"not_finite": float("nan")}, {"not_finite": float("inf")}):
            with self.subTest(state=state):
                with self.assertRaises(ValueError):
                    save_checkpoint(self.path, state)
                self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), original)


class CheckpointLoadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shared = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.shared.cleanup)
        cls.store = GitStore.create(Path(cls.shared.name) / "source.git", {"main.py": "print('ready')\n"})
        cls.head = cls.store.head()
        cls.files = cls.store.read_files(cls.head)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "checkpoint.json"
        self.ledger = Ledger(Path(self.temp.name) / "budget.sqlite", budget_units=10)
        self.ledger.add_task("completed-stage")
        self.lease = self.ledger.claim("completed-stage", "worker", now=0, ttl=10)
        self.ledger.reserve("settled-request", self.lease, 8, now=1)
        self.ledger.settle("settled-request", 3)
        self.state = {
            "contract_sha": "a" * 64, "pid": os.getpid(),
            "store_path": str(self.store.path), "head": self.head,
            "files": self.files, "next_stage": 1,
            "completed_stages": [{"completed": True, "id": "initial"}],
            "budget_ledger": str(self.ledger.path),
            "other_state": {"arbitrary": [1, "preserved"]},
        }

    def load(self, state=None, **overrides):
        sha = save_checkpoint(self.path, self.state if state is None else state)
        arguments = dict(expected_contract_sha=self.state["contract_sha"], expected_pid=os.getpid() + 1)
        arguments.update(overrides)
        return load_checkpoint(self.path, sha, **arguments)

    def test_loads_settled_handoff_without_changing_ledger(self):
        before = self.ledger.path.read_bytes()
        self.assertEqual(self.load(), self.state)
        self.assertEqual(self.ledger.path.read_bytes(), before)
        self.assertEqual(self.ledger.budget(), {"limit": 10, "spent_or_reserved": 3, "remaining": 7})

    def test_real_child_process_can_resume_checkpoint(self):
        sha = save_checkpoint(self.path, self.state)
        result = subprocess.run(
            [sys.executable, "-c", (
                "import os,sys; from gossip_harness.sustained_checkpoint import load_checkpoint; "
                "s=load_checkpoint(sys.argv[1],sys.argv[2],expected_contract_sha=sys.argv[3],expected_pid=os.getpid()); "
                "print(s['next_stage'])"
            ), str(self.path), sha, self.state["contract_sha"]],
            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "1")

    def test_tampered_bytes_wrong_contract_and_same_pid_reject(self):
        sha = save_checkpoint(self.path, self.state)
        self.path.write_bytes(self.path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            load_checkpoint(self.path, sha, expected_contract_sha=self.state["contract_sha"])
        with self.assertRaisesRegex(ValueError, "contract"):
            self.load(expected_contract_sha="b" * 64)
        with self.assertRaisesRegex(ValueError, "different process"):
            self.load(expected_pid=os.getpid())

    def test_head_and_file_mismatches_reject(self):
        state = deepcopy(self.state)
        state["head"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "head"):
            self.load(state)
        state = deepcopy(self.state)
        state["files"]["main.py"] += "# unexpected edit\n"
        with self.assertRaisesRegex(ValueError, "files"):
            self.load(state)
        state = deepcopy(self.state)
        state["files"]["untracked.py"] = "pass\n"
        with self.assertRaisesRegex(ValueError, "files"):
            self.load(state)

    def test_head_changed_during_read_rejects(self):
        with patch.object(GitStore, "head", side_effect=[self.head, "0" * 40]):
            with self.assertRaisesRegex(ValueError, "during verification"):
                self.load()

    def test_stage_completion_and_field_types_are_strict(self):
        invalid = [dict(next_stage=True), dict(next_stage=-1), dict(next_stage=4),
                   dict(next_stage=1.0), dict(next_stage=2),
                   dict(completed_stages=[{"completed": False}]),
                   dict(completed_stages=[{"completed": 1}]),
                   dict(completed_stages=[{}]), dict(completed_stages=[True]),
                   dict(pid=True), dict(pid=0), dict(store_path=""),
                   dict(budget_ledger=""), dict(head=None), dict(files={"main.py": None})]
        for change in invalid:
            with self.subTest(change=change):
                state = deepcopy(self.state)
                state.update(change)
                with self.assertRaises(ValueError):
                    self.load(state)
        for stage in (0, 3):
            state = deepcopy(self.state)
            state.update(next_stage=stage, completed_stages=[{"completed": True}] * stage)
            self.assertEqual(self.load(state), state)

    def test_unsettled_reservation_rejects_even_after_lease_expiry(self):
        self.ledger.reserve("ambiguous-call", self.lease, 4, now=2)
        self.ledger.claim("completed-stage", "replacement", now=11, ttl=10)
        with self.assertRaisesRegex(ValueError, "unsettled"):
            self.load()
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 7)

    def test_pending_promotion_rejects(self):
        self.ledger.begin_intent([self.lease], self.store.path, self.head, self.head, now=2)
        with self.assertRaisesRegex(ValueError, "pending promotion"):
            self.load()
        self.assertEqual(len(self.ledger.pending_intents()), 1)

    def test_missing_or_invalid_ledger_is_not_created_or_bootstrapped(self):
        missing = Path(self.temp.name) / "missing.sqlite"
        state = deepcopy(self.state)
        state["budget_ledger"] = str(missing)
        with self.assertRaisesRegex(ValueError, "does not exist"):
            self.load(state)
        self.assertFalse(missing.exists())
        invalid = Path(self.temp.name) / "invalid.sqlite"
        with sqlite3.connect(invalid) as db:
            db.execute("CREATE TABLE unrelated (value TEXT)")
        before = invalid.read_bytes()
        state["budget_ledger"] = str(invalid)
        with self.assertRaisesRegex(ValueError, "could not be verified"):
            self.load(state)
        self.assertEqual(invalid.read_bytes(), before)

    def test_duplicate_json_keys_reject_even_with_matching_hash(self):
        data = b'{"pid":1,"pid":2}'
        self.path.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "duplicate keys"):
            load_checkpoint(self.path, hashlib.sha256(data).hexdigest(), expected_contract_sha="a" * 64)


if __name__ == "__main__":
    unittest.main()
