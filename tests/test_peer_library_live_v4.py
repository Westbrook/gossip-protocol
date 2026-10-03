"""Offline operator-entry checks; only disposable wallets and fake credentials."""
from __future__ import annotations

from contextlib import ExitStack
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import FinancialError
from gossip_harness.peer_library_project_v4 import ProjectConfig
from gossip_harness import peer_library_live_v4 as live


class PeerLibraryLiveV4Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.wallet = self.root / "existing.sqlite"
        self.ledger = Ledger(self.wallet, 1_000_000)
        self.output = self.root / "observation"
        self.config = ProjectConfig()
        self.contract = {"execution_design": {"test": "design"}, "sources": {"test": "source"},
                         "execution_design_sha256": "a" * 64}
        self.permit = {**self.contract, "expected_global_cap": 1_000_000,
                       "expected_opening_usage": 0, "incremental_cap_micro_usd": 100_000}

    def entry_patches(self):
        stack = ExitStack()
        stack.enter_context(patch.object(live, "execution_contract", return_value=self.contract))
        stack.enter_context(patch.object(live, "enrollment", return_value={"cohort_id": "new-pilot"}))
        stack.enter_context(patch.object(live, "profile_specs", return_value={"test": "profile"}))
        stack.enter_context(patch.object(live, "preflight_permit", return_value={}))
        return stack

    def preflight(self):
        return live.preflight(self.output, self.wallet, self.permit, "b" * 64, self.config)

    def test_missing_existing_wallet_is_never_created(self):
        absent = self.root / "absent.sqlite"
        with self.assertRaises(FinancialError):
            live.opening_snapshot(absent, "new-pilot")
        self.assertFalse(absent.exists())

    def test_opening_snapshot_reads_prior_usage_without_changing_wallet(self):
        self.ledger.add_task("old")
        lease = self.ledger.claim("old", "actor", now=1, ttl=1)
        self.ledger.reserve("old-charge", lease, 2000, now=1)
        self.ledger.settle("old-charge", 700)
        with sqlite3.connect(self.wallet) as db:
            before = list(db.iterdump())
        answer = live.opening_snapshot(self.wallet, "new-pilot")
        with sqlite3.connect(self.wallet) as db:
            after = list(db.iterdump())
        self.assertEqual(before, after)
        self.assertEqual(answer["used_or_reserved"], 700)
        self.assertEqual(answer["unsettled_reservations"], 0)

    def test_failed_qualification_never_reads_key_or_enters_project(self):
        with self.entry_patches(), patch.object(live, "preflight_permit",
                side_effect=FinancialError("rehearsal is unqualified")), \
                patch.object(live, "load_authorized_key") as key, patch.object(live, "run_project") as run:
            with self.assertRaises(FinancialError):
                live.run_live(self.output, self.wallet, self.permit, "b" * 64, self.config)
            key.assert_not_called()
            run.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_preflight_is_keyless_and_rejects_wrong_opening_or_changed_design(self):
        with self.entry_patches(), patch.object(live, "load_authorized_key") as key:
            answer = self.preflight()
            self.assertTrue(answer["passed"])
            self.assertFalse(answer["credentials_read"])
            self.assertEqual(answer["api_calls"], 0)
            self.permit["expected_opening_usage"] = 1
            with self.assertRaises(FinancialError):
                self.preflight()
            self.permit["expected_opening_usage"] = 0
            self.permit["execution_design"] = {"changed": True}
            with self.assertRaises(FinancialError):
                self.preflight()
            key.assert_not_called()

    def test_unsettled_reservation_and_existing_cohort_refuse_fresh_start(self):
        self.ledger.add_task("old")
        lease = self.ledger.claim("old", "actor", now=1, ttl=1)
        self.ledger.reserve("unknown", lease, 2000, now=1)
        self.permit["expected_opening_usage"] = 2000
        with self.entry_patches(), self.assertRaises(FinancialError):
            self.preflight()
        self.ledger.settle("unknown", 2000)
        with sqlite3.connect(self.wallet) as db:
            db.execute("CREATE TABLE financial_cohorts_v2(cohort TEXT, halted INTEGER)")
            db.execute("INSERT INTO financial_cohorts_v2 VALUES ('new-pilot', 0)")
        with self.entry_patches(), self.assertRaises(FinancialError):
            self.preflight()

    def test_existing_output_and_indirect_input_are_refused(self):
        self.output.mkdir()
        with self.entry_patches(), self.assertRaises(FinancialError):
            self.preflight()
        indirect = self.root / "wallet-link"
        indirect.symlink_to(self.wallet)
        with self.assertRaises(FinancialError):
            live.opening_snapshot(indirect, "new-pilot")

    def test_literal_credential_file_is_ignored_untracked_and_never_exported(self):
        env = self.root / ".env.local"
        fake = "unit-test-placeholder-credential"
        env.write_text(f"UNRELATED=unused\nOPENAI_API_KEY='{fake}'\n")
        environment = {}
        replies = [subprocess.CompletedProcess([], 1), subprocess.CompletedProcess([], 0)]
        with patch.object(live, "REPOSITORY", self.root), patch.object(live.subprocess, "run", side_effect=replies):
            self.assertEqual(live.load_authorized_key(env, environment=environment), fake)
        self.assertEqual(environment, {})

    def test_secret_input_errors_do_not_echo_value_or_source(self):
        env = self.root / ".env.local"
        fake = "unit-test-placeholder-credential"
        for contents in (f"OPENAI_API_KEY={fake}\nOPENAI_API_KEY={fake}\n",
                         f"OPENAI_API_KEY=$(echo {fake})\n", f"OPENAI_API_KEY={fake} #comment\n"):
            env.write_text(contents)
            replies = [subprocess.CompletedProcess([], 1), subprocess.CompletedProcess([], 0)]
            with patch.object(live, "REPOSITORY", self.root), patch.object(live.subprocess, "run", side_effect=replies):
                with self.assertRaises(FinancialError) as caught:
                    live.load_authorized_key(env, environment={})
                self.assertNotIn(fake, str(caught.exception))

    def test_tracked_or_nonignored_secret_file_refuses_before_read(self):
        env = self.root / ".env.local"
        env.write_text("OPENAI_API_KEY=unit-test-placeholder\n")
        for tracked_code, ignored_code in ((0, 0), (1, 1), (128, 0)):
            replies = [subprocess.CompletedProcess([], tracked_code), subprocess.CompletedProcess([], ignored_code)]
            with patch.object(live, "REPOSITORY", self.root), patch.object(live.subprocess, "run", side_effect=replies), \
                    patch.object(Path, "open", side_effect=AssertionError("must not read")):
                with self.assertRaises(FinancialError):
                    live.load_authorized_key(env, environment={})

    def test_live_entry_uses_parent_memory_workers_after_gate_and_keeps_mode_explicit(self):
        events = []
        fake = "unit-test-placeholder-credential"
        with patch.object(live, "preflight", side_effect=lambda *args: events.append("qualified")), \
                patch.object(live, "load_authorized_key", side_effect=lambda *args: (events.append("key") or fake)), \
                patch.object(live, "run_project", return_value={"passed": False}) as run:
            self.assertEqual(live.run_live(self.output, self.wallet, self.permit, "b" * 64, self.config), {"passed": False})
            self.assertEqual(events, ["qualified", "key"])
            self.assertEqual(run.call_args.kwargs["mode"], "live")
            self.assertEqual(set(run.call_args.kwargs["workers"]), {"mini", "strong"})
            self.assertEqual(run.call_args.kwargs["ledger_path"], self.wallet)
            self.assertEqual(run.call_args.kwargs["expected_permit_sha256"], "b" * 64)

    def test_permit_rejects_duplicate_fields_and_cli_cannot_implicitly_start(self):
        permit = self.root / "permit.json"
        permit.write_text('{"mode":"live","mode":"fixture"}')
        with self.assertRaises(ValueError):
            live.read_permit(permit)
        with patch.object(live, "run_live") as run, patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit):
                live.main([])
            with self.assertRaises(SystemExit):
                live.main(["live"])
            run.assert_not_called()

    def test_contract_command_never_reads_credentials(self):
        with patch.object(live, "execution_contract", return_value=self.contract), \
                patch.object(live, "load_authorized_key") as key, patch("sys.stdout", new_callable=io.StringIO) as out:
            self.assertEqual(live.main(["contract"]), 0)
            self.assertEqual(json.loads(out.getvalue()), self.contract)
            key.assert_not_called()


if __name__ == "__main__":
    unittest.main()
